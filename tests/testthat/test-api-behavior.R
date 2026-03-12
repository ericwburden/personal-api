test_that("API auth and notes endpoints enforce expected behavior", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)

  health_resp <- perform_api_request(api, "GET", "/health")
  testthat::expect_equal(httr2::resp_status(health_resp), 200L)
  testthat::expect_equal(httr2::resp_body_json(health_resp)$status, "ok")

  unauthorized_notes <- perform_api_request(api, "GET", "/notes")
  testthat::expect_equal(httr2::resp_status(unauthorized_notes), 401L)

  invalid_json_resp <- perform_api_request(
    api,
    "POST",
    "/notes",
    token = api$token,
    body = "{invalid"
  )
  testthat::expect_equal(httr2::resp_status(invalid_json_resp), 400L)
  testthat::expect_equal(httr2::resp_body_json(invalid_json_resp)$error, "Invalid JSON body")

  missing_text_resp <- perform_api_request(
    api,
    "POST",
    "/notes",
    token = api$token,
    body = "{}"
  )
  testthat::expect_equal(httr2::resp_status(missing_text_resp), 400L)
  testthat::expect_equal(httr2::resp_body_json(missing_text_resp)$error, "Field 'text' is required")

  created_resp <- perform_api_request(
    api,
    "POST",
    "/notes",
    token = api$token,
    body = jsonlite::toJSON(list(text = "hello from tests"), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(created_resp), 200L)
  created_body <- httr2::resp_body_json(created_resp)
  testthat::expect_equal(created_body$status, "created")
  testthat::expect_equal(created_body$text, "hello from tests")

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
