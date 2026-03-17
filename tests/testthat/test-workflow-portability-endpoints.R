test_that("workflow portability endpoints export import and snapshot restore", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)

  put_context <- perform_api_request(
    api,
    "PUT",
    "/v1/contexts/portable-context",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Portable Context",
        content = list(summary = "orig"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_context), 200L)

  put_skill <- perform_api_request(
    api,
    "PUT",
    "/v1/skills/portable-skill",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Portable Skill",
        content = list(prompt = "do work"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(put_skill), 200L)

  put_automation <- perform_api_request(
    api,
    "PUT",
    "/v1/automations/portable-automation",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Portable Automation",
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
        source_id = "portable-automation",
        target_type = "skill",
        target_id = "portable-skill",
        required = TRUE
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(dep), 200L)

  tags <- perform_api_request(
    api,
    "PUT",
    "/v1/tags",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        object_type = "context",
        object_id = "portable-context",
        tags = c("portable", "backup")
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(tags), 200L)

  publish <- perform_api_request(
    api,
    "POST",
    "/v1/contexts/portable-context/publish",
    token = api$token,
    body = jsonlite::toJSON(list(updated_by = "tests"), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(publish), 200L)

  export_resp <- perform_api_request(
    api,
    "GET",
    "/v1/export?include_versions=true",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(export_resp), 200L)
  export_body <- httr2::resp_body_json(export_resp, simplifyVector = FALSE)
  testthat::expect_true(length(export_body$objects) >= 3)
  testthat::expect_true(length(export_body$versions) >= 1)

  create_snapshot <- perform_api_request(
    api,
    "POST",
    "/v1/snapshots?include_versions=true",
    token = api$token,
    body = jsonlite::toJSON(
      list(note = "portable snapshot", created_by = "tests"),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(create_snapshot), 200L)
  create_snapshot_body <- httr2::resp_body_json(create_snapshot, simplifyVector = TRUE)
  snapshot_id <- as.character(create_snapshot_body$snapshot_id)
  testthat::expect_true(nzchar(snapshot_id))

  list_snapshots <- perform_api_request(
    api,
    "GET",
    "/v1/snapshots",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(list_snapshots), 200L)
  list_snapshots_body <- httr2::resp_body_json(list_snapshots, simplifyVector = TRUE)
  testthat::expect_true(any(list_snapshots_body$snapshot_id == snapshot_id))

  get_snapshot <- perform_api_request(
    api,
    "GET",
    paste0("/v1/snapshots/", snapshot_id),
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(get_snapshot), 200L)
  get_snapshot_body <- httr2::resp_body_json(get_snapshot, simplifyVector = FALSE)
  testthat::expect_equal(as.character(get_snapshot_body$snapshot_id[[1]]), snapshot_id)
  testthat::expect_true(length(get_snapshot_body$payload$objects) >= 3)

  mutate_context <- perform_api_request(
    api,
    "PUT",
    "/v1/contexts/portable-context",
    token = api$token,
    body = jsonlite::toJSON(
      list(
        title = "Portable Context",
        content = list(summary = "mutated"),
        updated_by = "tests"
      ),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(mutate_context), 200L)

  restore_snapshot <- perform_api_request(
    api,
    "POST",
    paste0("/v1/snapshots/", snapshot_id, "/restore?strategy=replace"),
    token = api$token,
    body = jsonlite::toJSON(list(), auto_unbox = TRUE)
  )
  testthat::expect_equal(httr2::resp_status(restore_snapshot), 200L)
  restore_snapshot_body <- httr2::resp_body_json(restore_snapshot, simplifyVector = TRUE)
  testthat::expect_equal(as.character(restore_snapshot_body$status), "restored")

  context_after_restore <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/portable-context",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(context_after_restore), 200L)
  context_after_restore_body <- httr2::resp_body_json(context_after_restore, simplifyVector = FALSE)
  testthat::expect_equal(as.character(context_after_restore_body$content$summary[[1]]), "orig")

  import_stage <- perform_api_request(
    api,
    "POST",
    "/v1/import?workspace=personal&project=default&env=stage&strategy=replace",
    token = api$token,
    body = jsonlite::toJSON(
      list(payload = export_body),
      auto_unbox = TRUE
    )
  )
  testthat::expect_equal(httr2::resp_status(import_stage), 200L)
  import_stage_body <- httr2::resp_body_json(import_stage, simplifyVector = TRUE)
  testthat::expect_equal(as.character(import_stage_body$env), "stage")

  stage_context <- perform_api_request(
    api,
    "GET",
    "/v1/contexts/portable-context?workspace=personal&project=default&env=stage",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(stage_context), 200L)
})
