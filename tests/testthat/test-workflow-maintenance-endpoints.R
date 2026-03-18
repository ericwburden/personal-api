test_that("workflow maintenance endpoints return stats and support prune dry-runs", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)

  put_skill <- perform_api_request(
    api,
    "PUT",
    "/v1/skills/maintenance-skill",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Maintenance Skill",
        content = list(prompt = "Keep systems healthy"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_skill), 200L)

  put_automation <- perform_api_request(
    api,
    "PUT",
    "/v1/automations/maintenance-runner",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Maintenance Runner",
        trigger_type = "manual",
        enabled = TRUE,
        content = list(instructions = "run"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_automation), 200L)

  add_dependency <- perform_api_request(
    api,
    "PUT",
    "/v1/dependencies",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        source_type = "automation",
        source_id = "maintenance-runner",
        target_type = "skill",
        target_id = "maintenance-skill",
        required = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(add_dependency), 200L)

  execute_automation <- perform_api_request(
    api,
    "POST",
    "/v1/automations/maintenance-runner/execute",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(execute_automation), 200L)

  create_snapshot <- perform_api_request(
    api,
    "POST",
    "/v1/snapshots",
    token = api$token,
    body = jsonlite::toJSON(
      list(note = "maintenance test", created_by = "tests"),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(create_snapshot), 200L)

  stats <- perform_api_request(
    api,
    "GET",
    "/v1/workflows/stats",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(stats), 200L)
  stats_body <- httr2::resp_body_json(stats, simplifyVector = TRUE)
  testthat::expect_gte(as.integer(stats_body$counts$skills), 1L)
  testthat::expect_gte(as.integer(stats_body$counts$automations), 1L)
  testthat::expect_gte(as.integer(stats_body$counts$snapshots), 1L)
  testthat::expect_gte(as.integer(stats_body$counts$runs), 1L)
  testthat::expect_true(isTRUE(stats_body$tables_available$snapshots))
  testthat::expect_true(isTRUE(stats_body$tables_available$automation_runs))

  Sys.sleep(1.2)

  preview_run_prune <- perform_api_request(
    api,
    "POST",
    "/v1/runs/prune",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        older_than_days = 0,
        statuses = list("completed"),
        dry_run = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(preview_run_prune), 200L)
  preview_run_prune_body <- httr2::resp_body_json(preview_run_prune, simplifyVector = TRUE)
  testthat::expect_equal(as.character(preview_run_prune_body$status), "preview")
  testthat::expect_gte(as.integer(preview_run_prune_body$eligible_runs), 1L)

  prune_runs <- perform_api_request(
    api,
    "POST",
    "/v1/runs/prune",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        older_than_days = 0,
        statuses = list("completed"),
        dry_run = FALSE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(prune_runs), 200L)
  prune_runs_body <- httr2::resp_body_json(prune_runs, simplifyVector = TRUE)
  testthat::expect_equal(as.character(prune_runs_body$status), "pruned")
  testthat::expect_gte(as.integer(prune_runs_body$deleted_runs), 1L)

  runs_after_prune <- perform_api_request(
    api,
    "GET",
    "/v1/runs?status=completed",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(runs_after_prune), 200L)
  runs_after_prune_body <- httr2::resp_body_json(runs_after_prune, simplifyVector = FALSE)
  testthat::expect_equal(length(runs_after_prune_body), 0L)

  preview_snapshot_prune <- perform_api_request(
    api,
    "POST",
    "/v1/snapshots/prune",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        older_than_days = 0,
        dry_run = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(preview_snapshot_prune), 200L)
  preview_snapshot_prune_body <- httr2::resp_body_json(preview_snapshot_prune, simplifyVector = TRUE)
  testthat::expect_equal(as.character(preview_snapshot_prune_body$status), "preview")
  testthat::expect_gte(as.integer(preview_snapshot_prune_body$eligible_snapshots), 1L)

  prune_snapshots <- perform_api_request(
    api,
    "POST",
    "/v1/snapshots/prune",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        older_than_days = 0,
        dry_run = FALSE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(prune_snapshots), 200L)
  prune_snapshots_body <- httr2::resp_body_json(prune_snapshots, simplifyVector = TRUE)
  testthat::expect_equal(as.character(prune_snapshots_body$status), "pruned")
  testthat::expect_gte(as.integer(prune_snapshots_body$deleted_snapshots), 1L)

  snapshots_after_prune <- perform_api_request(
    api,
    "GET",
    "/v1/snapshots",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(snapshots_after_prune), 200L)
  snapshots_after_prune_body <- httr2::resp_body_json(snapshots_after_prune, simplifyVector = FALSE)
  testthat::expect_equal(length(snapshots_after_prune_body), 0L)
})
