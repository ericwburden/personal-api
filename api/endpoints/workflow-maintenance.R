#* Workflow maintenance endpoints for stats and retention cleanup.
#* Requires bearer authentication.

wf_run_status_values <- c("running", "completed", "failed", "cancelled")

wf_has_table <- function(table_name) {
  isTRUE(DBI::dbExistsTable(con, table_name))
}

wf_require_run_tables <- function(res) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  if (!wf_has_table("automation_runs")) {
    res$status <- 503
    return(list(error = "Missing required table 'automation_runs'. Run scripts/0000-init-duckdb.R."))
  }

  if (!wf_has_table("automation_run_logs")) {
    res$status <- 503
    return(list(error = "Missing required table 'automation_run_logs'. Run scripts/0000-init-duckdb.R."))
  }

  NULL
}

wf_scope_table_count <- function(table_name, scope) {
  out <- DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT COUNT(*) AS n
      FROM %s
      WHERE workspace = ? AND project = ? AND env = ?
      ",
      table_name
    ),
    params = list(scope$workspace, scope$project, scope$env)
  )

  as.integer(out$n[[1]] %||% 0L)
}

wf_count_run_logs <- function(scope) {
  if (!wf_has_table("automation_runs") || !wf_has_table("automation_run_logs")) {
    return(0L)
  }

  out <- DBI::dbGetQuery(
    con,
    "
    SELECT COUNT(*) AS n
    FROM automation_run_logs l
    INNER JOIN automation_runs r ON r.run_id = l.run_id
    WHERE r.workspace = ? AND r.project = ? AND r.env = ?
    ",
    params = list(scope$workspace, scope$project, scope$env)
  )

  as.integer(out$n[[1]] %||% 0L)
}

wf_stats_runs_by_status <- function(scope) {
  if (!wf_has_table("automation_runs")) {
    return(list())
  }

  rows <- DBI::dbGetQuery(
    con,
    "
    SELECT status, COUNT(*) AS n
    FROM automation_runs
    WHERE workspace = ? AND project = ? AND env = ?
    GROUP BY status
    ORDER BY status
    ",
    params = list(scope$workspace, scope$project, scope$env)
  )

  out <- list()
  if (nrow(rows) > 0) {
    for (i in seq_len(nrow(rows))) {
      key <- as.character(rows$status[[i]] %||% "")
      if (!nzchar(key)) {
        next
      }
      out[[key]] <- as.integer(rows$n[[i]] %||% 0L)
    }
  }

  out
}

wf_workflow_stats <- function(scope) {
  tables_available <- list(
    snapshots = wf_has_table("snapshots"),
    automation_runs = wf_has_table("automation_runs"),
    automation_run_logs = wf_has_table("automation_run_logs")
  )

  counts <- list(
    contexts = wf_scope_table_count("contexts", scope),
    skills = wf_scope_table_count("skills", scope),
    automations = wf_scope_table_count("automations", scope),
    versions = wf_scope_table_count("versions", scope),
    tags = wf_scope_table_count("tags", scope),
    dependencies = wf_scope_table_count("dependencies", scope),
    snapshots = if (isTRUE(tables_available$snapshots)) wf_scope_table_count("snapshots", scope) else 0L,
    runs = if (isTRUE(tables_available$automation_runs)) wf_scope_table_count("automation_runs", scope) else 0L,
    run_logs = wf_count_run_logs(scope)
  )

  list(
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    tables_available = tables_available,
    counts = counts,
    run_status_counts = wf_stats_runs_by_status(scope)
  )
}

wf_status_vector <- function(statuses, default = c("completed", "failed", "cancelled")) {
  values <- wf_to_character_vector(statuses)
  values <- trimws(tolower(values))
  values <- values[nzchar(values)]

  if (length(values) == 1L && grepl(",", values[[1]], fixed = TRUE)) {
    values <- trimws(unlist(strsplit(values[[1]], ",", fixed = TRUE), use.names = FALSE))
    values <- values[nzchar(values)]
  }

  values <- unique(values)
  if (length(values) == 0L) {
    return(unique(default))
  }

  values
}

wf_validate_run_status_filter <- function(res, statuses) {
  invalid <- statuses[!(statuses %in% wf_run_status_values)]
  if (length(invalid) > 0) {
    res$status <- 400
    return(list(error = paste0("Invalid run status values: ", paste(invalid, collapse = ", "))))
  }

  NULL
}

wf_run_prune_filter <- function(scope, statuses, cutoff_utc, alias = NULL) {
  prefix <- if (is.null(alias)) "" else paste0(alias, ".")

  status_placeholders <- paste(rep("?", length(statuses)), collapse = ", ")
  where <- paste(
    c(
      paste0(prefix, "workspace = ?"),
      paste0(prefix, "project = ?"),
      paste0(prefix, "env = ?"),
      paste0(prefix, "started_at < ?"),
      paste0(prefix, "status IN (", status_placeholders, ")")
    ),
    collapse = " AND "
  )

  params <- c(
    list(scope$workspace, scope$project, scope$env, cutoff_utc),
    as.list(statuses)
  )

  list(where = where, params = params)
}

wf_prune_runs <- function(scope, older_than_days, statuses, dry_run = TRUE) {
  cutoff_time <- as.POSIXct(Sys.time() - (older_than_days * 86400), tz = "UTC")
  cutoff_utc <- format(cutoff_time, "%Y-%m-%d %H:%M:%S", tz = "UTC")

  run_filter <- wf_run_prune_filter(scope, statuses, cutoff_utc, alias = NULL)
  run_filter_with_alias <- wf_run_prune_filter(scope, statuses, cutoff_utc, alias = "r")

  runs_count <- DBI::dbGetQuery(
    con,
    sprintf("SELECT COUNT(*) AS n FROM automation_runs WHERE %s", run_filter$where),
    params = run_filter$params
  )
  eligible_runs <- as.integer(runs_count$n[[1]] %||% 0L)

  logs_count <- DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT COUNT(*) AS n
      FROM automation_run_logs l
      WHERE l.run_id IN (
        SELECT r.run_id
        FROM automation_runs r
        WHERE %s
      )
      ",
      run_filter_with_alias$where
    ),
    params = run_filter_with_alias$params
  )
  eligible_logs <- as.integer(logs_count$n[[1]] %||% 0L)

  if (isTRUE(dry_run)) {
    return(
      list(
        status = "preview",
        workspace = scope$workspace,
        project = scope$project,
        env = scope$env,
        cutoff_utc = cutoff_utc,
        statuses = statuses,
        eligible_runs = eligible_runs,
        eligible_logs = eligible_logs,
        dry_run = TRUE
      )
    )
  }

  DBI::dbWithTransaction(
    con,
    {
      DBI::dbExecute(
        con,
        sprintf(
          "
          DELETE FROM automation_run_logs
          WHERE run_id IN (
            SELECT r.run_id
            FROM automation_runs r
            WHERE %s
          )
          ",
          run_filter_with_alias$where
        ),
        params = run_filter_with_alias$params
      )

      DBI::dbExecute(
        con,
        sprintf("DELETE FROM automation_runs WHERE %s", run_filter$where),
        params = run_filter$params
      )
    }
  )

  list(
    status = "pruned",
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    cutoff_utc = cutoff_utc,
    statuses = statuses,
    deleted_runs = eligible_runs,
    deleted_logs = eligible_logs,
    dry_run = FALSE
  )
}

wf_prune_snapshots <- function(scope, older_than_days, dry_run = TRUE) {
  cutoff_time <- as.POSIXct(Sys.time() - (older_than_days * 86400), tz = "UTC")
  cutoff_utc <- format(cutoff_time, "%Y-%m-%d %H:%M:%S", tz = "UTC")

  count_query <- DBI::dbGetQuery(
    con,
    "
    SELECT COUNT(*) AS n
    FROM snapshots
    WHERE workspace = ?
      AND project = ?
      AND env = ?
      AND created_at < ?
    ",
    params = list(scope$workspace, scope$project, scope$env, cutoff_utc)
  )
  eligible <- as.integer(count_query$n[[1]] %||% 0L)

  if (isTRUE(dry_run)) {
    return(
      list(
        status = "preview",
        workspace = scope$workspace,
        project = scope$project,
        env = scope$env,
        cutoff_utc = cutoff_utc,
        eligible_snapshots = eligible,
        dry_run = TRUE
      )
    )
  }

  DBI::dbExecute(
    con,
    "
    DELETE FROM snapshots
    WHERE workspace = ?
      AND project = ?
      AND env = ?
      AND created_at < ?
    ",
    params = list(scope$workspace, scope$project, scope$env, cutoff_utc)
  )

  list(
    status = "pruned",
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    cutoff_utc = cutoff_utc,
    deleted_snapshots = eligible,
    dry_run = FALSE
  )
}

#* Return scoped workflow storage and run statistics.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/workflows/stats
function(res, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_workflow_stats(wf_scope(workspace, project, env))
}

#* Prune run history for one scope.
#* Request body (optional): `{ "older_than_days": 30, "statuses": ["completed", "failed", "cancelled"], "dry_run": true }`.
#* `dry_run` defaults to `true`.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/runs/prune
function(req, res, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_require_run_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  parsed <- wf_json_parse_optional(req, res, default = list())
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  statuses <- wf_status_vector(parsed$statuses, default = c("completed", "failed", "cancelled"))
  status_error <- wf_validate_run_status_filter(res, statuses)
  if (!is.null(status_error)) {
    return(status_error)
  }

  scope <- wf_scope(workspace, project, env)
  older_than_days <- wf_clamp_int(parsed$older_than_days, default = 30L, min_value = 0L, max_value = 3650L)
  dry_run <- wf_bool(parsed$dry_run, default = TRUE)

  wf_prune_runs(scope = scope, older_than_days = older_than_days, statuses = statuses, dry_run = dry_run)
}

#* Prune snapshots for one scope.
#* Request body (optional): `{ "older_than_days": 90, "dry_run": true }`.
#* `dry_run` defaults to `true`.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/snapshots/prune
function(req, res, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  if (!wf_has_table("snapshots")) {
    res$status <- 503
    return(list(error = "Missing required table 'snapshots'. Run scripts/0000-init-duckdb.R."))
  }

  parsed <- wf_json_parse_optional(req, res, default = list())
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  scope <- wf_scope(workspace, project, env)
  older_than_days <- wf_clamp_int(parsed$older_than_days, default = 90L, min_value = 0L, max_value = 3650L)
  dry_run <- wf_bool(parsed$dry_run, default = TRUE)

  wf_prune_snapshots(scope = scope, older_than_days = older_than_days, dry_run = dry_run)
}
