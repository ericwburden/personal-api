test_that("API auth and notes endpoints enforce expected behavior", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)
  scalar_value <- function(x, key) as.character(x[[key]][[1]])

  health_resp <- perform_api_request(api, "GET", "/health")
  testthat::expect_equal(httr2::resp_status(health_resp), 200L)
  testthat::expect_equal(scalar_value(httr2::resp_body_json(health_resp), "status"), "ok")

  openapi_resp <- perform_api_request(api, "GET", "/openapi.json")
  testthat::expect_equal(httr2::resp_status(openapi_resp), 200L)
  openapi <- httr2::resp_body_json(openapi_resp, simplifyVector = FALSE)
  testthat::expect_true(!is.null(openapi$openapi))
  testthat::expect_true(!is.null(openapi$paths[["/hevy/summary"]]$get$responses[["200"]]$content$`application/json`$example))
  testthat::expect_true(!is.null(openapi$paths[["/hevy/timeline"]]$get$responses[["200"]]$content$`application/json`$example))
  testthat::expect_true(!is.null(openapi$paths[["/hevy/exercises"]]$get$responses[["200"]]$content$`application/json`$example))
  testthat::expect_true(!is.null(openapi$paths[["/hevy/exercises/{exercise_id}/history"]]$get$responses[["200"]]$content$`application/json`$example))

  swagger_redirect_resp <- httr2::request(paste0(api$base_url, "/swagger/")) |>
    httr2::req_error(is_error = function(resp) FALSE) |>
    httr2::req_timeout(10) |>
    httr2::req_options(followlocation = FALSE) |>
    httr2::req_perform()
  testthat::expect_equal(httr2::resp_status(swagger_redirect_resp), 302L)
  testthat::expect_equal(httr2::resp_header(swagger_redirect_resp, "location"), "/__docs__/")

  docs_resp <- perform_api_request(api, "GET", "/__docs__/")
  testthat::expect_equal(httr2::resp_status(docs_resp), 200L)
  content_type <- httr2::resp_header(docs_resp, "content-type")
  if (is.null(content_type) || !nzchar(content_type) || is.na(content_type)) {
    content_type <- ""
  }
  testthat::expect_match(
    content_type,
    "^text/html",
    perl = TRUE
  )
  docs_html <- httr2::resp_body_string(docs_resp)
  testthat::expect_gt(nchar(docs_html), 0)
  testthat::expect_match(docs_html, "openapi.json")

  unauthorized_notes <- perform_api_request(api, "GET", "/notes")
  testthat::expect_equal(httr2::resp_status(unauthorized_notes), 401L)

  unauthorized_hevy <- perform_api_request(api, "GET", "/hevy/workouts")
  testthat::expect_equal(httr2::resp_status(unauthorized_hevy), 401L)

  hevy_missing_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/workouts",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(hevy_missing_resp), 404L)
  hevy_missing_body <- httr2::resp_body_json(hevy_missing_resp)
  testthat::expect_match(
    scalar_value(hevy_missing_body, "error"),
    "Run Hevy sync first",
    fixed = TRUE
  )

  hevy_full_missing_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/workouts/example-workout/full",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(hevy_full_missing_resp), 404L)

  hevy_status_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/status",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(hevy_status_resp), 200L)
  hevy_status_body <- httr2::resp_body_json(hevy_status_resp, simplifyVector = TRUE)
  testthat::expect_true(all(c("last_backfill_at", "last_sync_at", "last_refresh_utc", "tables") %in% names(hevy_status_body)))
  testthat::expect_true(all(c("workouts", "workout_exercises", "sets", "routines") %in% names(hevy_status_body$tables)))
  testthat::expect_identical(hevy_status_body$tables$workouts$exists, FALSE)

  hevy_summary_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/summary",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(hevy_summary_resp), 404L)
  hevy_summary_body <- httr2::resp_body_json(hevy_summary_resp)
  testthat::expect_match(
    scalar_value(hevy_summary_body, "error"),
    "Run Hevy sync first",
    fixed = TRUE
  )

  hevy_timeline_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/timeline",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(hevy_timeline_resp), 404L)
  hevy_timeline_body <- httr2::resp_body_json(hevy_timeline_resp)
  testthat::expect_match(
    scalar_value(hevy_timeline_body, "error"),
    "Run Hevy sync first",
    fixed = TRUE
  )

  hevy_exercises_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/exercises",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(hevy_exercises_resp), 404L)
  hevy_exercises_body <- httr2::resp_body_json(hevy_exercises_resp)
  testthat::expect_match(
    scalar_value(hevy_exercises_body, "error"),
    "Run Hevy sync first",
    fixed = TRUE
  )

  hevy_exercise_history_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/exercises/example-exercise/history",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(hevy_exercise_history_resp), 404L)
  hevy_exercise_history_body <- httr2::resp_body_json(hevy_exercise_history_resp)
  testthat::expect_match(
    scalar_value(hevy_exercise_history_body, "error"),
    "Run Hevy sync first",
    fixed = TRUE
  )

  invalid_json_resp <- perform_api_request(
    api,
    "POST",
    "/notes",
    token = api$token,
    body = "not-json",
    content_type = "text/plain"
  )
  testthat::expect_equal(httr2::resp_status(invalid_json_resp), 400L)
  testthat::expect_equal(scalar_value(httr2::resp_body_json(invalid_json_resp), "error"), "Invalid JSON body")

  missing_text_resp <- perform_api_request(
    api,
    "POST",
    "/notes",
    token = api$token,
    body = "{}"
  )
  testthat::expect_equal(httr2::resp_status(missing_text_resp), 400L)
  testthat::expect_equal(scalar_value(httr2::resp_body_json(missing_text_resp), "error"), "Field 'text' is required")

  created_resp <- perform_api_request(
    api,
    "POST",
    "/notes",
    token = api$token,
    body = jsonlite::toJSON(list(text = "hello from tests"), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(created_resp), 200L)
  created_body <- httr2::resp_body_json(created_resp)
  testthat::expect_equal(scalar_value(created_body, "status"), "created")
  testthat::expect_equal(scalar_value(created_body, "text"), "hello from tests")

  notes_resp <- perform_api_request(
    api,
    "GET",
    "/notes?limit=10",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(notes_resp), 200L)
  notes <- httr2::resp_body_json(notes_resp, simplifyVector = TRUE)
  testthat::expect_true(any(notes$text == "hello from tests"))
})
