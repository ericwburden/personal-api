test_that("workflow batch endpoints upsert and publish multiple objects", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)

  batch_upsert <- perform_api_request(
    api,
    "POST",
    "/v1/objects/batch-upsert",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        objects = list(
          list(
            object_type = "context",
            object_id = "batch-context",
            title = "Batch Context",
            content = list(summary = "alpha"),
            updated_by = "tests"
          ),
          list(
            object_type = "skill",
            object_id = "batch-skill",
            title = "Batch Skill",
            content = list(prompt = "Do the thing"),
            updated_by = "tests"
          ),
          list(
            object_type = "automation",
            object_id = "batch-automation",
            title = "Batch Automation",
            trigger_type = "manual",
            enabled = TRUE,
            content = list(instructions = "run"),
            updated_by = "tests"
          )
        )
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(batch_upsert), 200L)
  batch_upsert_body <- httr2::resp_body_json(batch_upsert, simplifyVector = FALSE)
  testthat::expect_equal(as.integer(batch_upsert_body$saved_count[[1]]), 3L)
  testthat::expect_equal(as.integer(batch_upsert_body$error_count[[1]]), 0L)

  get_context <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/batch-context",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(get_context), 200L)

  batch_publish <- perform_api_request(
    api,
    "POST",
    "/v1/objects/batch-publish",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        updated_by = "tests",
        change_note = "batch publish",
        refs = list(
          list(object_type = "context", object_id = "batch-context"),
          list(object_type = "skill", object_id = "batch-skill")
        )
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(batch_publish), 200L)
  batch_publish_body <- httr2::resp_body_json(batch_publish, simplifyVector = FALSE)
  testthat::expect_equal(as.integer(batch_publish_body$published_count[[1]]), 2L)
  testthat::expect_equal(as.integer(batch_publish_body$error_count[[1]]), 0L)

  context_versions <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/batch-context/versions",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(context_versions), 200L)
  context_versions_body <- httr2::resp_body_json(context_versions, simplifyVector = TRUE)
  testthat::expect_equal(nrow(context_versions_body), 1L)

  partial_upsert <- perform_api_request(
    api,
    "POST",
    "/v1/objects/batch-upsert",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        continue_on_error = TRUE,
        objects = list(
          list(
            object_type = "not-real",
            object_id = "bad",
            title = "Bad"
          ),
          list(
            object_type = "context",
            object_id = "batch-context",
            title = "Batch Context Updated",
            content = list(summary = "beta"),
            updated_by = "tests"
          )
        )
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(partial_upsert), 207L)
  partial_upsert_body <- httr2::resp_body_json(partial_upsert, simplifyVector = FALSE)
  testthat::expect_equal(as.integer(partial_upsert_body$saved_count[[1]]), 1L)
  testthat::expect_equal(as.integer(partial_upsert_body$error_count[[1]]), 1L)

  context_after_partial <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/batch-context",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(context_after_partial), 200L)
  context_after_partial_body <- httr2::resp_body_json(context_after_partial, simplifyVector = FALSE)
  testthat::expect_equal(as.character(context_after_partial_body$content$summary[[1]]), "beta")

  missing_publish <- perform_api_request(
    api,
    "POST",
    "/v1/objects/batch-publish",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        refs = list(list(object_type = "skill", object_id = "does-not-exist"))
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(missing_publish), 400L)
  missing_publish_body <- httr2::resp_body_json(missing_publish, simplifyVector = FALSE)
  testthat::expect_equal(as.integer(missing_publish_body$published_count[[1]]), 0L)
  testthat::expect_equal(as.integer(missing_publish_body$error_count[[1]]), 1L)
})

test_that("workflow batch delete endpoint removes multiple objects with partial-error support", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)

  seed_objects <- perform_api_request(
    api,
    "POST",
    "/v1/objects/batch-upsert",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        objects = list(
          list(
            object_type = "context",
            object_id = "batch-delete-context",
            title = "Batch Delete Context",
            content = list(summary = "seed"),
            updated_by = "tests"
          ),
          list(
            object_type = "skill",
            object_id = "batch-delete-skill",
            title = "Batch Delete Skill",
            content = list(prompt = "seed"),
            updated_by = "tests"
          ),
          list(
            object_type = "automation",
            object_id = "batch-delete-auto",
            title = "Batch Delete Auto",
            trigger_type = "manual",
            enabled = TRUE,
            content = list(instructions = "seed"),
            updated_by = "tests"
          )
        )
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(seed_objects), 200L)

  publish_context <- perform_api_request(
    api,
    "POST",
    "/v1/contexts/batch-delete-context/publish",
    token = api$token,
    body = jsonlite::toJSON(list(updated_by = "tests"), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(publish_context), 200L)

  batch_delete <- perform_api_request(
    api,
    "POST",
    "/v1/objects/batch-delete",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        delete_versions = FALSE,
        refs = list(
          list(object_type = "context", object_id = "batch-delete-context"),
          list(object_type = "skill", object_id = "batch-delete-skill")
        )
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(batch_delete), 200L)
  batch_delete_body <- httr2::resp_body_json(batch_delete, simplifyVector = FALSE)
  testthat::expect_equal(as.integer(batch_delete_body$deleted_count[[1]]), 2L)
  testthat::expect_equal(as.integer(batch_delete_body$error_count[[1]]), 0L)

  context_after_delete <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/batch-delete-context",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(context_after_delete), 404L)

  context_versions <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/batch-delete-context/versions",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(context_versions), 200L)
  context_versions_body <- httr2::resp_body_json(context_versions, simplifyVector = TRUE)
  testthat::expect_equal(nrow(context_versions_body), 1L)

  partial_delete <- perform_api_request(
    api,
    "POST",
    "/v1/objects/batch-delete",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        continue_on_error = TRUE,
        refs = list(
          list(object_type = "skill", object_id = "does-not-exist"),
          list(object_type = "automation", object_id = "batch-delete-auto")
        )
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(partial_delete), 207L)
  partial_delete_body <- httr2::resp_body_json(partial_delete, simplifyVector = FALSE)
  testthat::expect_equal(as.integer(partial_delete_body$deleted_count[[1]]), 1L)
  testthat::expect_equal(as.integer(partial_delete_body$error_count[[1]]), 1L)
})
