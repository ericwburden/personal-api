seed_hevy_curated_fixture <- function(data_dir, project_dir) {
  hevy_dir <- file.path(data_dir, "curated", "hevy")
  dir.create(hevy_dir, recursive = TRUE, showWarnings = FALSE)

  workouts <- data.frame(
    workout_id = c("w1", "w2", "w3"),
    title = c("Push A", "Push B", "Leg Day"),
    description = c("Bench focus", "Bench volume", "Squat focus"),
    started_at = as.POSIXct(
      c("2026-01-02 12:00:00", "2026-01-04 14:00:00", "2026-02-10 16:30:00"),
      tz = "UTC"
    ),
    ended_at = as.POSIXct(
      c("2026-01-02 12:30:00", "2026-01-04 14:40:00", "2026-02-10 17:15:00"),
      tz = "UTC"
    ),
    duration_seconds = c(1800, 2400, 2700),
    created_at = as.POSIXct(
      c("2026-01-02 11:58:00", "2026-01-04 13:59:00", "2026-02-10 16:28:00"),
      tz = "UTC"
    ),
    updated_at = as.POSIXct(
      c("2026-01-02 12:31:00", "2026-01-04 14:42:00", "2026-02-10 17:16:00"),
      tz = "UTC"
    ),
    source = c("hevy", "hevy", "hevy"),
    stringsAsFactors = FALSE
  )

  workout_exercises <- data.frame(
    workout_id = c("w1", "w1", "w2", "w3", "w3"),
    exercise_index = c(1L, 2L, 1L, 1L, 2L),
    exercise_id = c("bench_press", "pull_up", "bench_press", "squat", "bench_press"),
    exercise_name = c("Bench Press", "Pull Up", "Bench Press", "Squat", "Bench Press"),
    notes = c("", "", "", "", ""),
    stringsAsFactors = FALSE
  )

  sets <- data.frame(
    workout_id = c("w1", "w1", "w1", "w2", "w2", "w2", "w3", "w3", "w3"),
    exercise_index = c(1L, 1L, 2L, 1L, 1L, 1L, 1L, 1L, 2L),
    set_index = c(1L, 2L, 1L, 1L, 2L, 3L, 1L, 2L, 1L),
    set_type = c("normal", "normal", "normal", "normal", "normal", "normal", "normal", "normal", "normal"),
    reps = c(5, 5, 8, 6, 6, 6, 5, 5, 4),
    weight_kg = c(100, 105, 0, 102.5, 102.5, 102.5, 140, 145, 110),
    distance_m = c(0, 0, 0, 0, 0, 0, 0, 0, 0),
    duration_seconds = c(0, 0, 0, 0, 0, 0, 0, 0, 0),
    rpe = c(8, 8.5, 7, 8, 8, 8, 9, 9, 8.5),
    is_warmup = c(FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE),
    notes = c("", "", "", "", "", "", "", "", ""),
    stringsAsFactors = FALSE
  )

  routines <- data.frame(
    routine_id = "r1",
    folder_id = "f1",
    routine_name = "Push Pull Legs",
    updated_at = as.POSIXct("2026-01-01 00:00:00", tz = "UTC"),
    stringsAsFactors = FALSE
  )

  arrow::write_parquet(workouts, file.path(hevy_dir, "workouts.parquet"))
  arrow::write_parquet(workout_exercises, file.path(hevy_dir, "workout_exercises.parquet"))
  arrow::write_parquet(sets, file.path(hevy_dir, "sets.parquet"))
  arrow::write_parquet(routines, file.path(hevy_dir, "routines.parquet"))
}

test_that("Hevy endpoints return expected aggregates with seeded curated data", {
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httr2")
  testthat::skip_if_not_installed("arrow")

  api <- start_test_api(
    token = "secret-token",
    setup_fn = seed_hevy_curated_fixture
  )
  on.exit(stop_test_api(api), add = TRUE)

  status_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/status",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(status_resp), 200L)
  status_body <- httr2::resp_body_json(status_resp, simplifyVector = TRUE)
  testthat::expect_true(isTRUE(status_body$tables$workouts$exists))
  testthat::expect_equal(as.integer(status_body$tables$workouts$row_count), 3L)
  testthat::expect_equal(as.integer(status_body$tables$workout_exercises$row_count), 5L)
  testthat::expect_equal(as.integer(status_body$tables$sets$row_count), 9L)
  testthat::expect_equal(as.integer(status_body$tables$routines$row_count), 1L)

  summary_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/summary?from=2026-01-01T00:00:00Z&to=2026-02-28T23:59:59Z",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(summary_resp), 200L)
  summary_body <- httr2::resp_body_json(summary_resp, simplifyVector = TRUE)
  testthat::expect_equal(as.integer(summary_body$workouts), 3L)
  testthat::expect_equal(as.integer(summary_body$training_days), 3L)
  testthat::expect_equal(as.numeric(summary_body$duration_seconds), 6900)
  testthat::expect_equal(as.integer(summary_body$sets), 9L)
  testthat::expect_equal(as.numeric(summary_body$reps), 50)
  testthat::expect_equal(as.numeric(summary_body$volume_kg), 4735)

  timeline_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/timeline?grain=month&from=2026-01-01T00:00:00Z&to=2026-02-28T23:59:59Z",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(timeline_resp), 200L)
  timeline <- httr2::resp_body_json(timeline_resp, simplifyVector = TRUE)
  testthat::expect_equal(nrow(timeline), 2L)
  testthat::expect_equal(as.integer(timeline$workouts[[1]]), 2L)
  testthat::expect_equal(as.numeric(timeline$volume_kg[[1]]), 2870)
  testthat::expect_equal(as.integer(timeline$workouts[[2]]), 1L)
  testthat::expect_equal(as.numeric(timeline$volume_kg[[2]]), 1865)

  exercises_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/exercises?sort_by=exercise_name&order=asc",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(exercises_resp), 200L)
  exercises <- httr2::resp_body_json(exercises_resp, simplifyVector = TRUE)
  testthat::expect_equal(nrow(exercises), 3L)
  bench <- exercises[exercises$exercise_id == "bench_press", , drop = FALSE]
  testthat::expect_equal(as.integer(bench$workouts[[1]]), 3L)
  testthat::expect_equal(as.integer(bench$sets[[1]]), 6L)
  testthat::expect_equal(as.numeric(bench$reps[[1]]), 32)
  testthat::expect_equal(as.numeric(bench$volume_kg[[1]]), 3310)

  exercises_search_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/exercises?search=bench",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(exercises_search_resp), 200L)
  exercises_search <- httr2::resp_body_json(exercises_search_resp, simplifyVector = TRUE)
  testthat::expect_equal(nrow(exercises_search), 1L)
  testthat::expect_equal(as.character(exercises_search$exercise_id[[1]]), "bench_press")

  history_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/exercises/bench_press/history?order=desc",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(history_resp), 200L)
  history <- httr2::resp_body_json(history_resp, simplifyVector = TRUE)
  testthat::expect_equal(nrow(history), 3L)
  testthat::expect_equal(as.character(history$workout_id[[1]]), "w3")
  h_w2 <- history[history$workout_id == "w2", , drop = FALSE]
  testthat::expect_equal(as.integer(h_w2$sets[[1]]), 3L)
  testthat::expect_equal(as.numeric(h_w2$reps[[1]]), 18)
  testthat::expect_equal(as.numeric(h_w2$top_weight_kg[[1]]), 102.5)
  testthat::expect_equal(as.numeric(h_w2$volume_kg[[1]]), 1845)

  missing_history_resp <- perform_api_request(
    api,
    "GET",
    "/hevy/exercises/does-not-exist/history",
    token = api$token
  )
  testthat::expect_equal(httr2::resp_status(missing_history_resp), 404L)
  missing_history <- httr2::resp_body_json(missing_history_resp, simplifyVector = TRUE)
  testthat::expect_equal(as.character(missing_history$error), "Exercise not found")
})
