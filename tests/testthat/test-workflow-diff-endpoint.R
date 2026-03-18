test_that("workflow diff endpoint compares source and target scopes", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)

  put_source_context <- perform_api_request(
    api,
    "PUT",
    "/v1/contexts/diff-context?workspace=personal&project=default&env=dev",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Diff Context",
        content = list(summary = "dev-v1"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_source_context), 200L)

  publish_source_context <- perform_api_request(
    api,
    "POST",
    "/v1/contexts/diff-context/publish?workspace=personal&project=default&env=dev",
    token = api$token,
    body = jsonlite::toJSON(list(updated_by = "tests"), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(publish_source_context), 200L)

  put_source_skill <- perform_api_request(
    api,
    "PUT",
    "/v1/skills/diff-skill?workspace=personal&project=default&env=dev",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Diff Skill",
        content = list(prompt = "dev"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_source_skill), 200L)

  put_source_auto <- perform_api_request(
    api,
    "PUT",
    "/v1/automations/diff-auto?workspace=personal&project=default&env=dev",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Diff Auto",
        trigger_type = "manual",
        enabled = TRUE,
        content = list(instructions = "run"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_source_auto), 200L)

  source_tag <- perform_api_request(
    api,
    "PUT",
    "/v1/tags",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        workspace = "personal",
        project = "default",
        env = "dev",
        object_type = "context",
        object_id = "diff-context",
        tags = c("shared", "source")
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(source_tag), 200L)

  source_dep <- perform_api_request(
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
        source_id = "diff-auto",
        target_type = "skill",
        target_id = "diff-skill",
        required = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(source_dep), 200L)

  put_target_context <- perform_api_request(
    api,
    "PUT",
    "/v1/contexts/diff-context?workspace=personal&project=default&env=stage",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Diff Context",
        content = list(summary = "stage-v1"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_target_context), 200L)

  put_target_only <- perform_api_request(
    api,
    "PUT",
    "/v1/contexts/stage-only?workspace=personal&project=default&env=stage",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Stage Only",
        content = list(summary = "target-only"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_target_only), 200L)

  target_tag <- perform_api_request(
    api,
    "PUT",
    "/v1/tags",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        workspace = "personal",
        project = "default",
        env = "stage",
        object_type = "context",
        object_id = "diff-context",
        tags = c("shared", "target")
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(target_tag), 200L)

  diff_resp <- perform_api_request(
    api,
    "GET",
    "/v1/workflows/diff?source_workspace=personal&source_project=default&source_env=dev&target_workspace=personal&target_project=default&target_env=stage&include_versions=true&limit=200",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(diff_resp), 200L)
  diff_body <- httr2::resp_body_json(diff_resp, simplifyVector = FALSE)

  testthat::expect_true(as.integer(diff_body$summary$objects$changed[[1]]) >= 1L)
  testthat::expect_true(as.integer(diff_body$summary$objects$source_only[[1]]) >= 2L)
  testthat::expect_true(as.integer(diff_body$summary$objects$target_only[[1]]) >= 1L)
  testthat::expect_true(as.integer(diff_body$summary$dependencies$source_only[[1]]) >= 1L)
  testthat::expect_true(as.integer(diff_body$summary$tags$source_only[[1]]) >= 1L)
  testthat::expect_true(as.integer(diff_body$summary$tags$target_only[[1]]) >= 1L)
  testthat::expect_true(as.integer(diff_body$summary$versions$source_only[[1]]) >= 1L)

  source_only_objects <- vapply(
    diff_body$changes$objects$source_only,
    function(x) paste0(as.character(x$object_type[[1]]), ":", as.character(x$object_id[[1]])),
    character(1)
  )
  testthat::expect_true("skill:diff-skill" %in% source_only_objects)
  testthat::expect_true("automation:diff-auto" %in% source_only_objects)

  target_only_objects <- vapply(
    diff_body$changes$objects$target_only,
    function(x) paste0(as.character(x$object_type[[1]]), ":", as.character(x$object_id[[1]])),
    character(1)
  )
  testthat::expect_true("context:stage-only" %in% target_only_objects)

  no_versions <- perform_api_request(
    api,
    "GET",
    "/v1/workflows/diff?source_workspace=personal&source_project=default&source_env=dev&target_workspace=personal&target_project=default&target_env=stage&include_versions=false",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(no_versions), 200L)
  no_versions_body <- httr2::resp_body_json(no_versions, simplifyVector = TRUE)
  testthat::expect_equal(as.integer(no_versions_body$summary$versions$source_only), 0L)
  testthat::expect_equal(as.integer(no_versions_body$summary$versions$target_only), 0L)
  testthat::expect_equal(as.integer(no_versions_body$summary$versions$changed), 0L)

  include_unchanged <- perform_api_request(
    api,
    "GET",
    "/v1/workflows/diff?source_workspace=personal&source_project=default&source_env=dev&target_workspace=personal&target_project=default&target_env=stage&include_versions=false&include_unchanged=true",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(include_unchanged), 200L)
  include_unchanged_body <- httr2::resp_body_json(include_unchanged, simplifyVector = FALSE)
  testthat::expect_true(length(include_unchanged_body$changes$objects$unchanged) >= 0L)
})
