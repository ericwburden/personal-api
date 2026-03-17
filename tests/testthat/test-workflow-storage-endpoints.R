test_that("workflow storage endpoints support personal persistent workflows", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)

  unauthorized <- perform_api_request(api, "GET", "/v1/contexts")
  testthat::expect_equal(httr2::resp_status(unauthorized), 401L)

  empty_contexts <- perform_api_request(
    api,
    "GET",
    "/v1/contexts",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(empty_contexts), 200L)
  empty_contexts_body <- httr2::resp_body_json(empty_contexts, simplifyVector = TRUE)
  testthat::expect_equal(as.integer(empty_contexts_body$total), 0L)

  put_context <- perform_api_request(
    api,
    "PUT",
    "/v1/contexts/session-context",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Session Context",
        content = list(summary = "alpha"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_context), 200L)
  put_context_body <- httr2::resp_body_json(put_context, simplifyVector = FALSE)
  testthat::expect_equal(as.character(put_context_body$status[[1]]), "saved")
  testthat::expect_equal(as.character(put_context_body$object$object_id[[1]]), "session-context")

  put_tags <- perform_api_request(
    api,
    "PUT",
    "/v1/tags",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        object_type = "context",
        object_id = "session-context",
        tags = c("session", "alpha"),
        workspace = "personal",
        project = "default",
        env = "dev"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_tags), 200L)
  put_tags_body <- httr2::resp_body_json(put_tags, simplifyVector = TRUE)
  testthat::expect_equal(length(put_tags_body$tags), 2L)

  tagged_contexts <- perform_api_request(
    api,
    "GET",
    "/v1/contexts?tag=session",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(tagged_contexts), 200L)
  tagged_contexts_body <- httr2::resp_body_json(tagged_contexts, simplifyVector = TRUE)
  testthat::expect_equal(as.integer(tagged_contexts_body$total), 1L)

  publish_context_v1 <- perform_api_request(
    api,
    "POST",
    "/v1/contexts/session-context/publish",
    token = api$token,
    body = jsonlite::toJSON(
      list(updated_by = "tests", change_note = "initial publish"),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(publish_context_v1), 200L)
  publish_context_v1_body <- httr2::resp_body_json(publish_context_v1, simplifyVector = TRUE)
  testthat::expect_equal(as.integer(publish_context_v1_body$version), 1L)

  update_context <- perform_api_request(
    api,
    "PUT",
    "/v1/contexts/session-context",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Session Context",
        content = list(summary = "beta"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(update_context), 200L)

  publish_context_v2 <- perform_api_request(
    api,
    "POST",
    "/v1/contexts/session-context/publish",
    token = api$token,
    body = jsonlite::toJSON(
      list(updated_by = "tests", change_note = "second publish"),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(publish_context_v2), 200L)
  publish_context_v2_body <- httr2::resp_body_json(publish_context_v2, simplifyVector = TRUE)
  testthat::expect_equal(as.integer(publish_context_v2_body$version), 2L)

  context_versions <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/session-context/versions",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(context_versions), 200L)
  context_versions_body <- httr2::resp_body_json(context_versions, simplifyVector = TRUE)
  testthat::expect_equal(nrow(context_versions_body), 2L)

  rollback_context <- perform_api_request(
    api,
    "POST",
    "/v1/contexts/session-context/rollback",
    token = api$token,
    body = jsonlite::toJSON(
      list(version = 1, updated_by = "tests"),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(rollback_context), 200L)

  context_after_rollback <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/session-context",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(context_after_rollback), 200L)
  context_after_rollback_body <- httr2::resp_body_json(context_after_rollback, simplifyVector = FALSE)
  testthat::expect_equal(as.character(context_after_rollback_body$content$summary[[1]]), "alpha")

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
        trigger_type = "cron",
        enabled = TRUE,
        content = list(schedule = "0 6 * * *"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_automation), 200L)

  dep_one <- perform_api_request(
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
  testthat::expect_equal(httr2::resp_status(dep_one), 200L)

  dep_two <- perform_api_request(
    api,
    "PUT",
    "/v1/dependencies",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        source_type = "automation",
        source_id = "daily-runner",
        target_type = "context",
        target_id = "session-context",
        required = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(dep_two), 200L)

  dependencies <- perform_api_request(
    api,
    "GET",
    "/v1/dependencies?source_type=automation&source_id=daily-runner",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(dependencies), 200L)
  dependencies_body <- httr2::resp_body_json(dependencies, simplifyVector = TRUE)
  testthat::expect_equal(nrow(dependencies_body), 2L)

  catalog_search <- perform_api_request(
    api,
    "GET",
    "/v1/catalog/search?q=runner",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(catalog_search), 200L)
  catalog_search_body <- httr2::resp_body_json(catalog_search, simplifyVector = FALSE)
  search_ids <- vapply(catalog_search_body$items, function(x) as.character(x$object_id[[1]]), character(1))
  testthat::expect_true("daily-runner" %in% search_ids)

  catalog_tree <- perform_api_request(
    api,
    "GET",
    "/v1/catalog/tree?group_by=type",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(catalog_tree), 200L)
  catalog_tree_body <- httr2::resp_body_json(catalog_tree, simplifyVector = FALSE)
  testthat::expect_equal(as.character(catalog_tree_body$group_by[[1]]), "type")
  testthat::expect_true(length(catalog_tree_body$groups$contexts) >= 1)
  testthat::expect_true(length(catalog_tree_body$groups$skills) >= 1)
  testthat::expect_true(length(catalog_tree_body$groups$automations) >= 1)

  catalog_resolve <- perform_api_request(
    api,
    "POST",
    "/v1/catalog/resolve",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        refs = list(list(object_type = "automation", object_id = "daily-runner")),
        include_dependencies = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(catalog_resolve), 200L)
  catalog_resolve_body <- httr2::resp_body_json(catalog_resolve, simplifyVector = FALSE)
  resolved_keys <- vapply(
    catalog_resolve_body$resolved,
    function(x) paste0(as.character(x$object_type[[1]]), ":", as.character(x$object_id[[1]])),
    character(1)
  )
  testthat::expect_true("automation:daily-runner" %in% resolved_keys)
  testthat::expect_true("skill:sql-skill" %in% resolved_keys)
  testthat::expect_true("context:session-context" %in% resolved_keys)
})
