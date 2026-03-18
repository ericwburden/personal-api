test_that("workflow graph endpoints detect cycles and produce execution plans", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("jsonlite")

  api <- start_test_api(token = "secret-token")
  on.exit(stop_test_api(api), add = TRUE)

  put_auto <- function(id) {
    perform_api_request(
      api,
      "PUT",
      paste0("/v1/automations/", id),
      token = api$token,
      body = jsonlite::toJSON(
        list(
          title = paste("Automation", id),
          trigger_type = "manual",
          enabled = TRUE,
          content = list(instructions = "run"),
          updated_by = "tests"
        ),
        auto_unbox = TRUE
      )
    )
  }

  for (id in c("plan-a", "plan-b", "plan-c", "cycle-x", "cycle-y")) {
    resp <- put_auto(id)
    testthat::expect_equal(httr2::resp_status(resp), 200L)
  }

  add_dep <- function(source_id, target_id, required = TRUE) {
    perform_api_request(
      api,
      "PUT",
      "/v1/dependencies",
      token = api$token,
      body = jsonlite::toJSON(
        list(
          source_type = "automation",
          source_id = source_id,
          target_type = "automation",
          target_id = target_id,
          required = required
        ),
        auto_unbox = TRUE
      )
    )
  }

  dep_ab <- add_dep("plan-a", "plan-b")
  testthat::expect_equal(httr2::resp_status(dep_ab), 200L)

  dep_bc <- add_dep("plan-b", "plan-c")
  testthat::expect_equal(httr2::resp_status(dep_bc), 200L)

  cycle_check_clean <- perform_api_request(
    api,
    "GET",
    "/v1/validate/dependency-cycles",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(cycle_check_clean), 200L)
  cycle_check_clean_body <- httr2::resp_body_json(cycle_check_clean, simplifyVector = TRUE)
  testthat::expect_true(isTRUE(cycle_check_clean_body$valid))
  testthat::expect_equal(as.integer(cycle_check_clean_body$cycle_count), 0L)

  plan_a <- perform_api_request(
    api,
    "GET",
    "/v1/automations/plan-a/execution-plan",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(plan_a), 200L)
  plan_a_body <- httr2::resp_body_json(plan_a, simplifyVector = TRUE)
  testthat::expect_equal(as.integer(plan_a_body$node_count), 3L)
  testthat::expect_equal(as.integer(plan_a_body$edge_count), 2L)
  testthat::expect_equal(as.character(plan_a_body$execution_order), c("plan-c", "plan-b", "plan-a"))

  dep_xy <- add_dep("cycle-x", "cycle-y")
  testthat::expect_equal(httr2::resp_status(dep_xy), 200L)

  dep_yx <- add_dep("cycle-y", "cycle-x")
  testthat::expect_equal(httr2::resp_status(dep_yx), 200L)

  cycle_check <- perform_api_request(
    api,
    "GET",
    "/v1/validate/dependency-cycles?limit=20",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(cycle_check), 200L)
  cycle_check_body <- httr2::resp_body_json(cycle_check, simplifyVector = FALSE)
  testthat::expect_false(isTRUE(cycle_check_body$valid[[1]]))
  testthat::expect_true(as.integer(cycle_check_body$cycle_count[[1]]) >= 1L)

  cycle_nodes <- unlist(
    lapply(
      cycle_check_body$cycles,
      function(x) vapply(x$path, function(y) as.character(y$object_id[[1]]), character(1))
    ),
    use.names = FALSE
  )
  testthat::expect_true(any(cycle_nodes %in% c("cycle-x", "cycle-y")))

  plan_cycle <- perform_api_request(
    api,
    "GET",
    "/v1/automations/cycle-x/execution-plan",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(plan_cycle), 409L)
  plan_cycle_body <- httr2::resp_body_json(plan_cycle, simplifyVector = TRUE)
  testthat::expect_true(grepl("cycle", as.character(plan_cycle_body$error), ignore.case = TRUE))

  missing_auto <- perform_api_request(
    api,
    "GET",
    "/v1/automations/not-real/execution-plan",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(missing_auto), 404L)
})
