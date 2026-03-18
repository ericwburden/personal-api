#* Workflow execution endpoints for automations.
#* Requires bearer authentication.

wf_run_terminal_status <- c("completed", "failed", "cancelled")

wf_require_execution_tables <- function(res) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  if (!isTRUE(DBI::dbExistsTable(con, "automation_runs"))) {
    res$status <- 503
    return(list(error = "Missing required table 'automation_runs'. Run scripts/0000-init-duckdb.R."))
  }

  if (!isTRUE(DBI::dbExistsTable(con, "automation_run_logs"))) {
    res$status <- 503
    return(list(error = "Missing required table 'automation_run_logs'. Run scripts/0000-init-duckdb.R."))
  }

  NULL
}

wf_next_run_log_line <- function(run_id) {
  out <- DBI::dbGetQuery(
    con,
    "
    SELECT COALESCE(MAX(line_no), 0) + 1 AS next_line
    FROM automation_run_logs
    WHERE run_id = ?
    ",
    params = list(run_id)
  )
  as.integer(out$next_line[[1]])
}

wf_append_run_log <- function(run_id, level, message) {
  DBI::dbExecute(
    con,
    "
    INSERT INTO automation_run_logs (run_id, line_no, level, message, created_at)
    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
    ",
    params = list(run_id, wf_next_run_log_line(run_id), tolower(trimws(as.character(level %||% "info"))), as.character(message %||% ""))
  )
}

wf_get_run_row <- function(run_id) {
  out <- DBI::dbGetQuery(
    con,
    "
    SELECT *
    FROM automation_runs
    WHERE run_id = ?
    LIMIT 1
    ",
    params = list(run_id)
  )

  if (nrow(out) == 0) {
    return(NULL)
  }
  out[1, , drop = FALSE]
}

wf_run_to_response <- function(row) {
  if (is.null(row) || nrow(row) == 0) {
    return(NULL)
  }

  list(
    run_id = as.character(row$run_id[[1]]),
    workspace = as.character(row$workspace[[1]]),
    project = as.character(row$project[[1]]),
    env = as.character(row$env[[1]]),
    automation_id = as.character(row$automation_id[[1]]),
    status = as.character(row$status[[1]]),
    started_at = as.character(row$started_at[[1]]),
    finished_at = as.character(row$finished_at[[1]] %||% ""),
    requested_by = as.character(row$requested_by[[1]] %||% ""),
    dry_run = isTRUE(row$dry_run[[1]] %||% FALSE),
    input = wf_json_decode(as.character(row$input_json[[1]] %||% "")),
    result = wf_json_decode(as.character(row$result_json[[1]] %||% "")),
    error = as.character(row$error_text[[1]] %||% "")
  )
}

wf_create_run <- function(scope, automation_id, requested_by, dry_run, input) {
  run_id <- uuid::UUIDgenerate()
  input_json <- wf_json_encode(input %||% list())

  DBI::dbExecute(
    con,
    "
    INSERT INTO automation_runs (
      run_id, workspace, project, env, automation_id, status, started_at,
      requested_by, dry_run, input_json, created_at
    )
    VALUES (?, ?, ?, ?, ?, 'running', CURRENT_TIMESTAMP, ?, ?, ?, CURRENT_TIMESTAMP)
    ",
    params = list(run_id, scope$workspace, scope$project, scope$env, automation_id, requested_by, isTRUE(dry_run), input_json)
  )

  wf_append_run_log(run_id, "info", paste0("Run started for automation '", automation_id, "'."))
  run_id
}

wf_finish_run <- function(run_id, status, result = NULL, error_text = NULL) {
  DBI::dbExecute(
    con,
    "
    UPDATE automation_runs
    SET status = ?,
        result_json = ?,
        error_text = ?,
        finished_at = CURRENT_TIMESTAMP
    WHERE run_id = ?
    ",
    params = list(status, wf_json_encode(result %||% list()), as.character(error_text %||% ""), run_id)
  )
}

wf_execute_automation <- function(res, automation_id, scope, requested_by = "", dry_run = FALSE, input = NULL) {
  id <- wf_id(automation_id)
  if (is.null(id)) {
    res$status <- 400
    return(list(error = "Automation id is required"))
  }

  automation_row <- wf_find_object("automation", id, scope)
  if (is.null(automation_row)) {
    res$status <- 404
    return(list(error = "Automation not found"))
  }

  automation_obj <- wf_object_to_response("automation", automation_row, scope = scope)
  run_id <- wf_create_run(scope, id, requested_by = requested_by, dry_run = dry_run, input = input)

  validation <- wf_validate_automation_object(automation_obj, scope)
  if (!isTRUE(validation$valid)) {
    wf_append_run_log(run_id, "error", "Validation failed; run marked as failed.")
    for (issue in wf_to_character_vector(validation$issues)) {
      wf_append_run_log(run_id, "error", issue)
    }
    wf_finish_run(
      run_id,
      status = "failed",
      result = list(
        automation_id = id,
        validation = validation
      ),
      error_text = "Validation failed"
    )

    res$status <- 422
    return(wf_run_to_response(wf_get_run_row(run_id)))
  }

  for (warn in wf_to_character_vector(validation$warnings)) {
    wf_append_run_log(run_id, "warn", warn)
  }

  result <- list(
    automation_id = id,
    executed = TRUE,
    dry_run = isTRUE(dry_run),
    dependency_count = as.integer(validation$dependency_count %||% 0L),
    resolved_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  )

  wf_append_run_log(run_id, "info", if (isTRUE(dry_run)) "Dry-run execution completed." else "Execution completed.")
  wf_finish_run(run_id, status = "completed", result = result, error_text = "")

  wf_run_to_response(wf_get_run_row(run_id))
}

#* Execute one automation and persist run metadata.
#* Request body (optional): `{ "requested_by": "...", "dry_run": true, "input": {...} }`.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/automations/<automation_id>/execute
function(req, res, automation_id, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_require_execution_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  parsed <- wf_json_parse_optional(req, res, default = list())
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  scope <- wf_scope(workspace, project, env)
  wf_execute_automation(
    res = res,
    automation_id = automation_id,
    scope = scope,
    requested_by = trimws(as.character(parsed$requested_by %||% "")),
    dry_run = wf_bool(parsed$dry_run, default = FALSE),
    input = parsed$input %||% list()
  )
}

#* List automation runs in scope.
#* @tag Workflows
#* @param limit:int Maximum rows (1..1000).
#* @param offset:int Rows to skip.
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/runs
function(res, workspace = "personal", project = "default", env = "dev", automation_id = NULL, status = NULL, limit = 100, offset = 0, order = "desc") {
  missing <- wf_require_execution_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  limit <- wf_clamp_int(limit, default = 100L, min_value = 1L, max_value = 1000L)
  offset <- wf_clamp_int(offset, default = 0L, min_value = 0L, max_value = 100000L)
  order <- wf_sort_order(order, default = "desc")

  where <- c("workspace = ?", "project = ?", "env = ?")
  params <- list(workspace, project, env)

  automation_id <- wf_id(automation_id)
  if (!is.null(automation_id)) {
    where <- c(where, "automation_id = ?")
    params <- c(params, list(automation_id))
  }

  status_val <- trimws(as.character(status %||% ""))
  if (nzchar(status_val)) {
    where <- c(where, "status = ?")
    params <- c(params, list(tolower(status_val)))
  }

  rows <- DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT *
      FROM automation_runs
      WHERE %s
      ORDER BY started_at %s, run_id ASC
      LIMIT %d
      OFFSET %d
      ",
      paste(where, collapse = " AND "),
      order,
      limit,
      offset
    ),
    params = params
  )

  lapply(seq_len(nrow(rows)), function(i) wf_run_to_response(rows[i, , drop = FALSE]))
}

#* Get one run by id.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/runs/<run_id>
function(res, run_id) {
  missing <- wf_require_execution_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  id <- wf_id(run_id)
  if (is.null(id)) {
    res$status <- 400
    return(list(error = "Run id is required"))
  }

  row <- wf_get_run_row(id)
  if (is.null(row)) {
    res$status <- 404
    return(list(error = "Run not found"))
  }

  wf_run_to_response(row)
}

#* List logs for one run.
#* @tag Workflows
#* @param limit:int Maximum rows (1..5000).
#* @param offset:int Rows to skip.
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/runs/<run_id>/logs
function(res, run_id, limit = 500, offset = 0) {
  missing <- wf_require_execution_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  id <- wf_id(run_id)
  if (is.null(id)) {
    res$status <- 400
    return(list(error = "Run id is required"))
  }

  if (is.null(wf_get_run_row(id))) {
    res$status <- 404
    return(list(error = "Run not found"))
  }

  limit <- wf_clamp_int(limit, default = 500L, min_value = 1L, max_value = 5000L)
  offset <- wf_clamp_int(offset, default = 0L, min_value = 0L, max_value = 100000L)

  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT run_id, line_no, level, message, created_at
      FROM automation_run_logs
      WHERE run_id = ?
      ORDER BY line_no ASC
      LIMIT %d
      OFFSET %d
      ",
      limit,
      offset
    ),
    params = list(id)
  )
}

#* Cancel a run if it is still active.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/runs/<run_id>/cancel
function(res, run_id) {
  missing <- wf_require_execution_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  id <- wf_id(run_id)
  if (is.null(id)) {
    res$status <- 400
    return(list(error = "Run id is required"))
  }

  row <- wf_get_run_row(id)
  if (is.null(row)) {
    res$status <- 404
    return(list(error = "Run not found"))
  }

  current_status <- as.character(row$status[[1]] %||% "")
  if (current_status %in% wf_run_terminal_status) {
    res$status <- 409
    return(list(error = paste0("Run is already ", current_status)))
  }

  wf_append_run_log(id, "warn", "Run cancelled by request.")
  wf_finish_run(id, status = "cancelled", result = wf_json_decode(as.character(row$result_json[[1]] %||% "")), error_text = as.character(row$error_text[[1]] %||% ""))

  wf_run_to_response(wf_get_run_row(id))
}
