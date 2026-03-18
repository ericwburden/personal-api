test_that("workflow object delete endpoints remove objects and clean related rows", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)

  put_context <- perform_api_request(
    api,
    "PUT",
    "/v1/contexts/delete-context",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Delete Context",
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
    "/v1/skills/delete-skill",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Delete Skill",
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
    "/v1/automations/delete-automation",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Delete Automation",
        trigger_type = "manual",
        enabled = TRUE,
        content = list(instructions = "run"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_automation), 200L)

  tag_context <- perform_api_request(
    api,
    "PUT",
    "/v1/tags",
    token = api$token,
    body = jsonlite::toJSON(
      list(object_type = "context", object_id = "delete-context", tags = c("cleanup", "ctx")),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(tag_context), 200L)

  tag_skill <- perform_api_request(
    api,
    "PUT",
    "/v1/tags",
    token = api$token,
    body = jsonlite::toJSON(
      list(object_type = "skill", object_id = "delete-skill", tags = c("cleanup", "skill")),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(tag_skill), 200L)

  dep_one <- perform_api_request(
    api,
    "PUT",
    "/v1/dependencies",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        source_type = "automation",
        source_id = "delete-automation",
        target_type = "skill",
        target_id = "delete-skill",
        required = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(dep_one), 200L)

  dep_two <- perform_api_request(
    api,
    "PUT",
    "/v1/dependencies",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        source_type = "automation",
        source_id = "delete-automation",
        target_type = "context",
        target_id = "delete-context",
        required = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(dep_two), 200L)

  publish_context <- perform_api_request(
    api,
    "POST",
    "/v1/contexts/delete-context/publish",
    token = api$token,
    body = jsonlite::toJSON(list(updated_by = "tests"), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(publish_context), 200L)

  publish_skill <- perform_api_request(
    api,
    "POST",
    "/v1/skills/delete-skill/publish",
    token = api$token,
    body = jsonlite::toJSON(list(updated_by = "tests"), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(publish_skill), 200L)

  publish_automation <- perform_api_request(
    api,
    "POST",
    "/v1/automations/delete-automation/publish",
    token = api$token,
    body = jsonlite::toJSON(list(updated_by = "tests"), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(publish_automation), 200L)

  delete_skill <- perform_api_request(
    api,
    "DELETE",
    "/v1/skills/delete-skill",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(delete_skill), 200L)
  delete_skill_body <- httr2::resp_body_json(delete_skill, simplifyVector = TRUE)
  testthat::expect_equal(as.character(delete_skill_body$status), "deleted")
  testthat::expect_true(isTRUE(delete_skill_body$delete_versions))

  get_skill <- perform_api_request(
    api,
    "GET",
    "/v1/skills/delete-skill",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(get_skill), 404L)

  skill_versions <- perform_api_request(
    api,
    "GET",
    "/v1/skills/delete-skill/versions",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(skill_versions), 200L)
  skill_versions_body <- httr2::resp_body_json(skill_versions, simplifyVector = FALSE)
  testthat::expect_equal(length(skill_versions_body), 0L)

  deps_after_skill_delete <- perform_api_request(
    api,
    "GET",
    "/v1/dependencies?source_type=automation&source_id=delete-automation",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(deps_after_skill_delete), 200L)
  deps_after_skill_delete_body <- httr2::resp_body_json(deps_after_skill_delete, simplifyVector = TRUE)
  testthat::expect_equal(nrow(deps_after_skill_delete_body), 1L)

  delete_context_keep_versions <- perform_api_request(
    api,
    "DELETE",
    "/v1/contexts/delete-context?delete_versions=false",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(delete_context_keep_versions), 200L)
  delete_context_body <- httr2::resp_body_json(delete_context_keep_versions, simplifyVector = TRUE)
  testthat::expect_false(isTRUE(delete_context_body$delete_versions))

  get_context <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/delete-context",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(get_context), 404L)

  context_versions <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/delete-context/versions",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(context_versions), 200L)
  context_versions_body <- httr2::resp_body_json(context_versions, simplifyVector = TRUE)
  testthat::expect_equal(nrow(context_versions_body), 1L)

  deps_after_context_delete <- perform_api_request(
    api,
    "GET",
    "/v1/dependencies?source_type=automation&source_id=delete-automation",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(deps_after_context_delete), 200L)
  deps_after_context_delete_body <- httr2::resp_body_json(deps_after_context_delete, simplifyVector = FALSE)
  testthat::expect_equal(length(deps_after_context_delete_body), 0L)

  delete_automation <- perform_api_request(
    api,
    "DELETE",
    "/v1/automations/delete-automation",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(delete_automation), 200L)

  get_automation <- perform_api_request(
    api,
    "GET",
    "/v1/automations/delete-automation",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(get_automation), 404L)

  delete_missing <- perform_api_request(
    api,
    "DELETE",
    "/v1/skills/does-not-exist",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(delete_missing), 404L)
})
