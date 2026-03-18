test_that("workflow activity endpoint returns cross-surface events and supports filters", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)

  put_context <- perform_api_request(
    api,
    "PUT",
    "/v1/contexts/activity-context",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Activity Context",
        content = list(summary = "ctx"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_context), 200L)

  put_skill <- perform_api_request(
    api,
    "PUT",
    "/v1/skills/activity-skill",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Activity Skill",
        content = list(prompt = "skill"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_skill), 200L)

  put_automation <- perform_api_request(
    api,
    "PUT",
    "/v1/automations/activity-automation",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Activity Automation",
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
        source_id = "activity-automation",
        target_type = "skill",
        target_id = "activity-skill",
        required = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(dep), 200L)

  publish_context <- perform_api_request(
    api,
    "POST",
    "/v1/contexts/activity-context/publish",
    token = api$token,
    body = jsonlite::toJSON(
      list(updated_by = "tests", change_note = "ctx publish"),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(publish_context), 200L)

  execute <- perform_api_request(
    api,
    "POST",
    "/v1/automations/activity-automation/execute",
    token = api$token,
    body = jsonlite::toJSON(
      list(requested_by = "tests", dry_run = TRUE),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(execute), 200L)

  snapshot <- perform_api_request(
    api,
    "POST",
    "/v1/snapshots",
    token = api$token,
    body = jsonlite::toJSON(
      list(note = "activity snapshot", created_by = "tests"),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(snapshot), 200L)

  activity <- perform_api_request(
    api,
    "GET",
    "/v1/workflows/activity?limit=200",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(activity), 200L)
  activity_body <- httr2::resp_body_json(activity, simplifyVector = FALSE)
  testthat::expect_true(length(activity_body$items) >= 6L)

  event_types <- vapply(activity_body$items, function(x) as.character(x$event_type[[1]]), character(1))
  testthat::expect_true("draft_saved" %in% event_types)
  testthat::expect_true("published" %in% event_types)
  testthat::expect_true("run_started" %in% event_types)
  testthat::expect_true("run_finished" %in% event_types)
  testthat::expect_true("snapshot_created" %in% event_types)

  filtered_runs <- perform_api_request(
    api,
    "GET",
    "/v1/workflows/activity?event_type=run_finished&object_type=automation&object_id=activity-automation&limit=20",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(filtered_runs), 200L)
  filtered_runs_body <- httr2::resp_body_json(filtered_runs, simplifyVector = FALSE)
  testthat::expect_true(length(filtered_runs_body$items) >= 1L)
  run_event_types <- vapply(filtered_runs_body$items, function(x) as.character(x$event_type[[1]]), character(1))
  testthat::expect_true(all(run_event_types == "run_finished"))

  invalid_event_type <- perform_api_request(
    api,
    "GET",
    "/v1/workflows/activity?event_type=not-real",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(invalid_event_type), 400L)

  future_window <- perform_api_request(
    api,
    "GET",
    "/v1/workflows/activity?from=2100-01-01T00:00:00Z",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(future_window), 200L)
  future_window_body <- httr2::resp_body_json(future_window, simplifyVector = FALSE)
  testthat::expect_equal(as.integer(future_window_body$total[[1]]), 0L)
})
