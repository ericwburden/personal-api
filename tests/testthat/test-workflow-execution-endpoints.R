test_that("workflow execution endpoints persist runs and logs", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)

  put_skill <- perform_api_request(
    api,
    "PUT",
    "/v1/skills/sql-skill",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "SQL Skill",
        content = list(prompt = "Write concise SQL"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_skill), 200L)

  put_automation <- perform_api_request(
    api,
    "PUT",
    "/v1/automations/daily-runner",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Daily Runner",
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
        source_id = "daily-runner",
        target_type = "skill",
        target_id = "sql-skill",
        required = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(add_dependency), 200L)

  execute_ok <- perform_api_request(
    api,
    "POST",
    "/v1/automations/daily-runner/execute",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        requested_by = "tests",
        dry_run = TRUE,
        input = list(reason = "smoke")
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(execute_ok), 200L)
  execute_ok_body <- httr2::resp_body_json(execute_ok, simplifyVector = FALSE)
  run_id <- as.character(execute_ok_body$run_id[[1]])
  testthat::expect_equal(as.character(execute_ok_body$status[[1]]), "completed")
  testthat::expect_true(isTRUE(execute_ok_body$dry_run[[1]]))
  testthat::expect_true(isTRUE(execute_ok_body$result$executed[[1]]))

  get_run <- perform_api_request(
    api,
    "GET",
    paste0("/v1/runs/", run_id),
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(get_run), 200L)
  get_run_body <- httr2::resp_body_json(get_run, simplifyVector = FALSE)
  testthat::expect_equal(as.character(get_run_body$run_id[[1]]), run_id)

  list_runs <- perform_api_request(
    api,
    "GET",
    "/v1/runs?automation_id=daily-runner&status=completed&limit=10",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(list_runs), 200L)
  list_runs_body <- httr2::resp_body_json(list_runs, simplifyVector = FALSE)
  listed_ids <- vapply(list_runs_body, function(x) as.character(x$run_id[[1]]), character(1))
  testthat::expect_true(run_id %in% listed_ids)

  run_logs <- perform_api_request(
    api,
    "GET",
    paste0("/v1/runs/", run_id, "/logs"),
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(run_logs), 200L)
  run_logs_body <- httr2::resp_body_json(run_logs, simplifyVector = TRUE)
  testthat::expect_true(nrow(run_logs_body) >= 2L)
  testthat::expect_true(any(grepl("Run started", run_logs_body$message, fixed = TRUE)))

  cancel_completed <- perform_api_request(
    api,
    "POST",
    paste0("/v1/runs/", run_id, "/cancel"),
    token = api$token,
    body = jsonlite::toJSON(list(), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(cancel_completed), 409L)

  missing_run <- perform_api_request(
    api,
    "GET",
    "/v1/runs/not-a-real-run-id",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(missing_run), 404L)

  missing_logs <- perform_api_request(
    api,
    "GET",
    "/v1/runs/not-a-real-run-id/logs",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(missing_logs), 404L)

  missing_cancel <- perform_api_request(
    api,
    "POST",
    "/v1/runs/not-a-real-run-id/cancel",
    token = api$token,
    body = jsonlite::toJSON(list(), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(missing_cancel), 404L)
})
