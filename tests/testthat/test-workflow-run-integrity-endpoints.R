test_that("workflow run-integrity endpoints detect and repair orphaned runs", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)

  put_skill <- perform_api_request(
    api,
    "PUT",
    "/v1/skills/run-integrity-skill",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Run Integrity Skill",
        content = list(prompt = "hello"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_skill), 200L)

  put_automation <- perform_api_request(
    api,
    "PUT",
    "/v1/automations/run-integrity-auto",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Run Integrity Auto",
        trigger_type = "manual",
        enabled = TRUE,
        content = list(instructions = "run"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_automation), 200L)

  dep <- perform_api_request(
    api,
    "PUT",
    "/v1/dependencies",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        source_type = "automation",
        source_id = "run-integrity-auto",
        target_type = "skill",
        target_id = "run-integrity-skill",
        required = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(dep), 200L)

  execute <- perform_api_request(
    api,
    "POST",
    "/v1/automations/run-integrity-auto/execute",
    token = api$token,
    body = jsonlite::toJSON(list(requested_by = "tests", dry_run = TRUE), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(execute), 200L)
  execute_body <- httr2::resp_body_json(execute, simplifyVector = FALSE)
  run_id <- as.character(execute_body$run_id[[1]])

  integrity_before_delete <- perform_api_request(
    api,
    "GET",
    "/v1/validate/run-integrity",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(integrity_before_delete), 200L)
  integrity_before_delete_body <- httr2::resp_body_json(integrity_before_delete, simplifyVector = TRUE)
  testthat::expect_true(isTRUE(integrity_before_delete_body$valid))
  testthat::expect_equal(as.integer(integrity_before_delete_body$counts$missing_automation_runs), 0L)

  delete_auto <- perform_api_request(
    api,
    "DELETE",
    "/v1/automations/run-integrity-auto",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(delete_auto), 200L)

  integrity_after_delete <- perform_api_request(
    api,
    "GET",
    "/v1/validate/run-integrity?limit=50",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(integrity_after_delete), 200L)
  integrity_after_delete_body <- httr2::resp_body_json(integrity_after_delete, simplifyVector = FALSE)
  testthat::expect_false(isTRUE(integrity_after_delete_body$valid[[1]]))
  testthat::expect_true(as.integer(integrity_after_delete_body$counts$missing_automation_runs[[1]]) >= 1L)

  repair_preview <- perform_api_request(
    api,
    "POST",
    "/v1/repair/run-integrity",
    token = api$token,
    body = jsonlite::toJSON(list(dry_run = TRUE), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(repair_preview), 200L)
  repair_preview_body <- httr2::resp_body_json(repair_preview, simplifyVector = TRUE)
  testthat::expect_equal(as.character(repair_preview_body$status), "preview")
  testthat::expect_true(as.integer(repair_preview_body$eligible$runs) >= 1L)

  repair_apply <- perform_api_request(
    api,
    "POST",
    "/v1/repair/run-integrity",
    token = api$token,
    body = jsonlite::toJSON(list(dry_run = FALSE), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(repair_apply), 200L)
  repair_apply_body <- httr2::resp_body_json(repair_apply, simplifyVector = TRUE)
  testthat::expect_equal(as.character(repair_apply_body$status), "repaired")
  testthat::expect_true(as.integer(repair_apply_body$deleted$runs) >= 1L)

  integrity_after_repair <- perform_api_request(
    api,
    "GET",
    "/v1/validate/run-integrity",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(integrity_after_repair), 200L)
  integrity_after_repair_body <- httr2::resp_body_json(integrity_after_repair, simplifyVector = TRUE)
  testthat::expect_true(isTRUE(integrity_after_repair_body$valid))
  testthat::expect_equal(as.integer(integrity_after_repair_body$counts$missing_automation_runs), 0L)

  run_after_repair <- perform_api_request(
    api,
    "GET",
    paste0("/v1/runs/", run_id),
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(run_after_repair), 404L)
})
