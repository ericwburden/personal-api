set_json_response_example <- function(spec, path, method = "get", status = "200", example = NULL) {
  if (is.null(spec$paths[[path]]) || is.null(spec$paths[[path]][[method]])) {
    return(spec)
  }

  response <- spec$paths[[path]][[method]]$responses[[status]]
  if (is.null(response)) {
    return(spec)
  }

  if (is.null(response$content)) {
    response$content <- list()
  }

  if (is.null(response$content$`application/json`)) {
    response$content$`application/json` <- list(schema = list(type = "object"))
  }

  response$content$`application/json`$example <- example
  spec$paths[[path]][[method]]$responses[[status]] <- response
  spec
}

add_openapi_bearer_auth <- function(spec) {
  if (is.null(spec$components)) {
    spec$components <- list()
  }

  if (is.null(spec$components$securitySchemes)) {
    spec$components$securitySchemes <- list()
  }

  spec$components$securitySchemes$bearerAuth <- list(
    type = "http",
    scheme = "bearer",
    bearerFormat = "API token"
  )

  # Default every operation to bearer auth unless explicitly overridden.
  spec$security <- list(list(bearerAuth = list()))

  public_ops <- list(
    list(path = "/health", method = "get"),
    list(path = "/swagger/", method = "get")
  )

  for (op in public_ops) {
    if (!is.null(spec$paths[[op$path]]) && !is.null(spec$paths[[op$path]][[op$method]])) {
      # Empty security array indicates this operation is public.
      spec$paths[[op$path]][[op$method]]$security <- list()
    }
  }

  spec
}

add_hevy_openapi_examples <- function(spec) {
  examples <- list(
    list(
      path = "/hevy/status",
      status = "200",
      example = list(
        last_backfill_at = "2026-03-01T12:00:00Z",
        last_sync_at = "2026-03-16T22:30:00Z",
        last_refresh_utc = "2026-03-16T22:31:03Z",
        tables = list(
          workouts = list(exists = TRUE, row_count = 128L),
          workout_exercises = list(exists = TRUE, row_count = 504L),
          sets = list(exists = TRUE, row_count = 1678L),
          routines = list(exists = TRUE, row_count = 14L)
        )
      )
    ),
    list(
      path = "/hevy/summary",
      status = "200",
      example = list(
        from = "2026-01-01T00:00:00Z",
        to = "2026-12-31T23:59:59Z",
        workouts = 72L,
        training_days = 54L,
        duration_seconds = 109220,
        sets = 1642L,
        reps = 10852,
        volume_kg = 483912.5
      )
    ),
    list(
      path = "/hevy/summary",
      status = "404",
      example = list(error = "Hevy table 'hevy.workouts' is not available. Run Hevy sync first.")
    ),
    list(
      path = "/hevy/timeline",
      status = "200",
      example = list(
        list(
          period_start = "2026-01-01T00:00:00Z",
          workouts = 9L,
          training_days = 8L,
          duration_seconds = 14520,
          sets = 218L,
          reps = 1476,
          volume_kg = 62155
        ),
        list(
          period_start = "2026-02-01T00:00:00Z",
          workouts = 11L,
          training_days = 9L,
          duration_seconds = 16940,
          sets = 255L,
          reps = 1652,
          volume_kg = 70294.5
        )
      )
    ),
    list(
      path = "/hevy/exercises",
      status = "200",
      example = list(
        list(
          exercise_id = "bench_press",
          exercise_name = "Bench Press",
          workouts = 24L,
          sets = 118L,
          reps = 734,
          volume_kg = 72854,
          last_performed = "2026-03-15T18:22:10Z"
        ),
        list(
          exercise_id = "pull_up",
          exercise_name = "Pull Up",
          workouts = 19L,
          sets = 81L,
          reps = 642,
          volume_kg = 0,
          last_performed = "2026-03-14T13:07:41Z"
        )
      )
    ),
    list(
      path = "/hevy/exercises",
      status = "404",
      example = list(error = "Hevy table 'hevy.workout_exercises' is not available. Run Hevy sync first.")
    ),
    list(
      path = "/hevy/exercises/{exercise_id}/history",
      status = "200",
      example = list(
        list(
          exercise_id = "bench_press",
          exercise_name = "Bench Press",
          workout_id = "wk_001",
          workout_title = "Push Day",
          workout_time = "2026-03-15T18:22:10Z",
          sets = 5L,
          reps = 32,
          top_weight_kg = 92.5,
          volume_kg = 2585
        ),
        list(
          exercise_id = "bench_press",
          exercise_name = "Bench Press",
          workout_id = "wk_002",
          workout_title = "Upper Body",
          workout_time = "2026-03-12T17:55:00Z",
          sets = 4L,
          reps = 24,
          top_weight_kg = 90,
          volume_kg = 2040
        )
      )
    ),
    list(
      path = "/hevy/exercises/{exercise_id}/history",
      status = "404",
      example = list(error = "Exercise not found")
    ),
    list(
      path = "/hevy/workouts",
      status = "200",
      example = list(
        list(
          workout_id = "wk_123",
          title = "Push Day",
          description = "Chest and shoulders",
          started_at = "2026-03-15T18:22:10Z",
          ended_at = "2026-03-15T19:09:04Z",
          duration_seconds = 2814,
          created_at = "2026-03-15T18:20:55Z",
          updated_at = "2026-03-15T19:09:14Z"
        )
      )
    ),
    list(
      path = "/hevy/workouts/{workout_id}",
      status = "200",
      example = list(
        list(
          workout_id = "wk_123",
          title = "Push Day",
          description = "Chest and shoulders",
          started_at = "2026-03-15T18:22:10Z",
          ended_at = "2026-03-15T19:09:04Z",
          duration_seconds = 2814,
          created_at = "2026-03-15T18:20:55Z",
          updated_at = "2026-03-15T19:09:14Z"
        )
      )
    ),
    list(
      path = "/hevy/workouts/{workout_id}",
      status = "404",
      example = list(error = "Workout not found")
    ),
    list(
      path = "/hevy/workouts/{workout_id}/full",
      status = "200",
      example = list(
        workout = list(
          list(
            workout_id = "wk_123",
            title = "Push Day",
            description = "Chest and shoulders",
            started_at = "2026-03-15T18:22:10Z",
            ended_at = "2026-03-15T19:09:04Z",
            duration_seconds = 2814,
            created_at = "2026-03-15T18:20:55Z",
            updated_at = "2026-03-15T19:09:14Z"
          )
        ),
        exercises = list(
          list(
            workout_id = "wk_123",
            exercise_index = 1L,
            exercise_id = "bench_press",
            exercise_name = "Bench Press",
            notes = ""
          )
        ),
        sets = list(
          list(
            workout_id = "wk_123",
            exercise_index = 1L,
            set_index = 1L,
            set_type = "normal",
            reps = 8,
            weight_kg = 80,
            distance_m = 0,
            duration_seconds = 0,
            rpe = 8,
            is_warmup = FALSE,
            notes = ""
          )
        )
      )
    ),
    list(
      path = "/hevy/workout-exercises",
      status = "200",
      example = list(
        list(
          workout_id = "wk_123",
          exercise_index = 1L,
          exercise_id = "bench_press",
          exercise_name = "Bench Press",
          notes = ""
        )
      )
    ),
    list(
      path = "/hevy/sets",
      status = "200",
      example = list(
        list(
          workout_id = "wk_123",
          exercise_index = 1L,
          set_index = 1L,
          set_type = "normal",
          reps = 8,
          weight_kg = 80,
          distance_m = 0,
          duration_seconds = 0,
          rpe = 8,
          is_warmup = FALSE,
          notes = ""
        )
      )
    ),
    list(
      path = "/hevy/routines",
      status = "200",
      example = list(
        list(
          routine_id = "rt_001",
          folder_id = "fd_001",
          routine_name = "Push Pull Legs",
          updated_at = "2026-03-01T10:02:44Z"
        )
      )
    )
  )

  for (ex in examples) {
    spec <- set_json_response_example(
      spec = spec,
      path = ex$path,
      method = "get",
      status = ex$status,
      example = ex$example
    )
  }

  spec
}

register_openapi_examples <- function(pr) {
  plumber::pr_set_api_spec(
    pr,
    function(spec) {
      spec <- add_openapi_bearer_auth(spec)
      add_hevy_openapi_examples(spec)
    }
  )
}
