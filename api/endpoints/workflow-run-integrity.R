#* Workflow run-integrity endpoints for validating and repairing orphaned runs.
#* Requires bearer authentication.

wf_missing_automation_runs_where <- function(scope) {
  list(
    where = "r.workspace = ? AND r.project = ? AND r.env = ? AND a.automation_id IS NULL",
    params = list(scope$workspace, scope$project, scope$env)
  )
}

wf_missing_automation_runs_count <- function(scope) {
  filter <- wf_missing_automation_runs_where(scope)

  out <- DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT COUNT(*) AS n
      FROM automation_runs r
      LEFT JOIN automations a
        ON a.workspace = r.workspace
       AND a.project = r.project
       AND a.env = r.env
       AND a.automation_id = r.automation_id
      WHERE %s
      ",
      filter$where
    ),
    params = filter$params
  )

  as.integer(out$n[[1]] %||% 0L)
}

wf_missing_automation_runs_sample <- function(scope, limit = 200L) {
  filter <- wf_missing_automation_runs_where(scope)
  limit <- wf_clamp_int(limit, default = 200L, min_value = 1L, max_value = 5000L)

  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT
        r.run_id,
        r.workspace,
        r.project,
        r.env,
        r.automation_id,
        r.status,
        r.started_at,
        r.finished_at,
        r.requested_by
      FROM automation_runs r
      LEFT JOIN automations a
        ON a.workspace = r.workspace
       AND a.project = r.project
       AND a.env = r.env
       AND a.automation_id = r.automation_id
      WHERE %s
      ORDER BY r.started_at DESC, r.run_id ASC
      LIMIT %d
      ",
      filter$where,
      limit
    ),
    params = filter$params
  )
}

wf_missing_automation_run_logs_count <- function(scope) {
  filter <- wf_missing_automation_runs_where(scope)

  out <- DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT COUNT(*) AS n
      FROM automation_run_logs l
      WHERE l.run_id IN (
        SELECT r.run_id
        FROM automation_runs r
        LEFT JOIN automations a
          ON a.workspace = r.workspace
         AND a.project = r.project
         AND a.env = r.env
         AND a.automation_id = r.automation_id
        WHERE %s
      )
      ",
      filter$where
    ),
    params = filter$params
  )

  as.integer(out$n[[1]] %||% 0L)
}

wf_validate_run_integrity <- function(scope, limit = 200L) {
  missing_count <- wf_missing_automation_runs_count(scope)
  sample_rows <- wf_missing_automation_runs_sample(scope, limit = limit)

  list(
    valid = missing_count == 0L,
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    counts = list(
      missing_automation_runs = as.integer(missing_count)
    ),
    missing_automation_runs = sample_rows
  )
}

wf_repair_run_integrity <- function(scope, dry_run = TRUE, limit = 200L) {
  missing_count <- wf_missing_automation_runs_count(scope)
  log_count <- wf_missing_automation_run_logs_count(scope)
  sample_rows <- wf_missing_automation_runs_sample(scope, limit = limit)

  if (isTRUE(dry_run)) {
    return(
      list(
        status = "preview",
        workspace = scope$workspace,
        project = scope$project,
        env = scope$env,
        dry_run = TRUE,
        eligible = list(
          runs = as.integer(missing_count),
          logs = as.integer(log_count)
        ),
        missing_automation_runs = sample_rows
      )
    )
  }

  filter <- wf_missing_automation_runs_where(scope)

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
            LEFT JOIN automations a
              ON a.workspace = r.workspace
             AND a.project = r.project
             AND a.env = r.env
             AND a.automation_id = r.automation_id
            WHERE %s
          )
          ",
          filter$where
        ),
        params = filter$params
      )

      DBI::dbExecute(
        con,
        sprintf(
          "
          DELETE FROM automation_runs
          WHERE run_id IN (
            SELECT r.run_id
            FROM automation_runs r
            LEFT JOIN automations a
              ON a.workspace = r.workspace
             AND a.project = r.project
             AND a.env = r.env
             AND a.automation_id = r.automation_id
            WHERE %s
          )
          ",
          filter$where
        ),
        params = filter$params
      )
    }
  )

  list(
    status = "repaired",
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    dry_run = FALSE,
    deleted = list(
      runs = as.integer(missing_count),
      logs = as.integer(log_count)
    )
  )
}

#* Validate run integrity by checking for runs whose automation no longer exists.
#* @tag Workflows
#* @param limit:int Maximum broken runs returned (1..5000).
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/validate/run-integrity
function(res, workspace = "personal", project = "default", env = "dev", limit = 200) {
  missing <- wf_require_execution_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_validate_run_integrity(
    scope = wf_scope(workspace, project, env),
    limit = wf_clamp_int(limit, default = 200L, min_value = 1L, max_value = 5000L)
  )
}

#* Repair run integrity by deleting runs/logs whose automation no longer exists.
#* Request body (optional): `{ "dry_run": true, "limit": 200 }`.
#* `dry_run` defaults to `true`.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/repair/run-integrity
function(req, res, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_require_execution_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  parsed <- wf_json_parse_optional(req, res, default = list())
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  wf_repair_run_integrity(
    scope = wf_scope(workspace, project, env),
    dry_run = wf_bool(parsed$dry_run, default = TRUE),
    limit = wf_clamp_int(parsed$limit, default = 200L, min_value = 1L, max_value = 5000L)
  )
}
