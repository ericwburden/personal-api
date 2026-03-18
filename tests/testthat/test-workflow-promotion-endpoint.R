test_that("workflow promotion endpoint previews and applies cross-scope sync", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)

  put_context <- perform_api_request(
    api,
    "PUT",
    "/v1/contexts/promote-context?workspace=personal&project=default&env=dev",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Promote Context",
        content = list(summary = "source-v1"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_context), 200L)

  publish_context <- perform_api_request(
    api,
    "POST",
    "/v1/contexts/promote-context/publish?workspace=personal&project=default&env=dev",
    token = api$token,
    body = jsonlite::toJSON(list(updated_by = "tests"), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(publish_context), 200L)

  put_skill <- perform_api_request(
    api,
    "PUT",
    "/v1/skills/promote-skill?workspace=personal&project=default&env=dev",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Promote Skill",
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
    "/v1/automations/promote-automation?workspace=personal&project=default&env=dev",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Promote Automation",
        trigger_type = "manual",
        enabled = TRUE,
        content = list(instructions = "run"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_automation), 200L)

  add_dep <- perform_api_request(
    api,
    "PUT",
    "/v1/dependencies",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        workspace = "personal",
        project = "default",
        env = "dev",
        source_type = "automation",
        source_id = "promote-automation",
        target_type = "skill",
        target_id = "promote-skill",
        required = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(add_dep), 200L)

  preview <- perform_api_request(
    api,
    "POST",
    "/v1/promote",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        source_workspace = "personal",
        source_project = "default",
        source_env = "dev",
        target_workspace = "personal",
        target_project = "default",
        target_env = "stage",
        strategy = "replace",
        include_versions = TRUE,
        dry_run = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(preview), 200L)
  preview_body <- httr2::resp_body_json(preview, simplifyVector = FALSE)
  testthat::expect_equal(as.character(preview_body$status[[1]]), "preview")
  testthat::expect_true(isTRUE(preview_body$dry_run[[1]]))
  testthat::expect_gte(as.integer(preview_body$payload_counts$objects[[1]]), 3L)

  stage_before <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/promote-context?workspace=personal&project=default&env=stage",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(stage_before), 404L)

  promote <- perform_api_request(
    api,
    "POST",
    "/v1/promote",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        source_workspace = "personal",
        source_project = "default",
        source_env = "dev",
        target_workspace = "personal",
        target_project = "default",
        target_env = "stage",
        strategy = "replace",
        include_versions = TRUE,
        dry_run = FALSE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(promote), 200L)
  promote_body <- httr2::resp_body_json(promote, simplifyVector = TRUE)
  testthat::expect_equal(as.character(promote_body$status), "promoted")
  testthat::expect_false(isTRUE(promote_body$dry_run))
  testthat::expect_gte(as.integer(promote_body$imported$objects), 3L)

  stage_context <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/promote-context?workspace=personal&project=default&env=stage",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(stage_context), 200L)
  stage_context_body <- httr2::resp_body_json(stage_context, simplifyVector = FALSE)
  testthat::expect_equal(as.character(stage_context_body$content$summary[[1]]), "source-v1")

  update_source <- perform_api_request(
    api,
    "PUT",
    "/v1/contexts/promote-context?workspace=personal&project=default&env=dev",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Promote Context",
        content = list(summary = "source-v2"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(update_source), 200L)

  promote_again <- perform_api_request(
    api,
    "POST",
    "/v1/promote",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        source_workspace = "personal",
        source_project = "default",
        source_env = "dev",
        target_workspace = "personal",
        target_project = "default",
        target_env = "stage",
        strategy = "upsert",
        include_versions = FALSE,
        dry_run = FALSE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(promote_again), 200L)

  stage_context_after <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/promote-context?workspace=personal&project=default&env=stage",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(stage_context_after), 200L)
  stage_context_after_body <- httr2::resp_body_json(stage_context_after, simplifyVector = FALSE)
  testthat::expect_equal(as.character(stage_context_after_body$content$summary[[1]]), "source-v2")

  invalid_strategy <- perform_api_request(
    api,
    "POST",
    "/v1/promote",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        source_workspace = "personal",
        source_project = "default",
        source_env = "dev",
        target_workspace = "personal",
        target_project = "default",
        target_env = "stage",
        strategy = "bad-strategy"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(invalid_strategy), 400L)
})
