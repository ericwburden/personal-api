#* Hevy API endpoints backed by curated DuckDB views.
#* Requires bearer authentication.

hevy_table_exists <- function(table) {
  DBI::dbExistsTable(con, DBI::Id(schema = "hevy", table = table))
}

missing_hevy_table <- function(res, table) {
  res$status <- 404
  list(error = paste0("Hevy table 'hevy.", table, "' is not available. Run Hevy sync first."))
}

#* List Hevy workouts ordered by most recent start time.
#* Requires bearer authentication.
#* @tag Hevy
#* @param limit:int Maximum records to return (clamped to 1..1000).
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy workouts returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Hevy curated workouts table is unavailable.
#* @get /hevy/workouts
function(res, limit = 100) {
  if (!hevy_table_exists("workouts")) {
    return(missing_hevy_table(res, "workouts"))
  }

  limit <- as.integer(limit)
  if (is.na(limit) || limit < 1) {
    limit <- 100
  }
  if (limit > 1000) {
    limit <- 1000
  }

  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT
        workout_id,
        title,
        description,
        started_at,
        ended_at,
        duration_seconds,
        created_at,
        updated_at
      FROM hevy.workouts
      ORDER BY COALESCE(started_at, created_at) DESC
      LIMIT %d
      ",
      limit
    )
  )
}

#* Get one Hevy workout by id.
#* Requires bearer authentication.
#* @tag Hevy
#* @param workout_id Workout id from Hevy.
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy workout returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Workout not found or Hevy workouts table unavailable.
#* @get /hevy/workouts/<workout_id>
function(res, workout_id) {
  if (!hevy_table_exists("workouts")) {
    return(missing_hevy_table(res, "workouts"))
  }

  out <- DBI::dbGetQuery(
    con,
    "
    SELECT
      workout_id,
      title,
      description,
      started_at,
      ended_at,
      duration_seconds,
      created_at,
      updated_at
    FROM hevy.workouts
    WHERE workout_id = ?
    LIMIT 1
    ",
    params = list(workout_id)
  )

  if (nrow(out) == 0) {
    res$status <- 404
    return(list(error = "Workout not found"))
  }

  out
}

#* List Hevy workout exercises.
#* Requires bearer authentication.
#* @tag Hevy
#* @param workout_id Filter to one workout id (optional).
#* @param limit:int Maximum records to return (clamped to 1..5000).
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy workout exercises returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Hevy curated workout_exercises table is unavailable.
#* @get /hevy/workout-exercises
function(res, workout_id = NULL, limit = 500) {
  if (!hevy_table_exists("workout_exercises")) {
    return(missing_hevy_table(res, "workout_exercises"))
  }

  limit <- as.integer(limit)
  if (is.na(limit) || limit < 1) {
    limit <- 500
  }
  if (limit > 5000) {
    limit <- 5000
  }

  if (!is.null(workout_id) && nzchar(trimws(workout_id))) {
    return(
      DBI::dbGetQuery(
        con,
        sprintf(
          "
          SELECT
            workout_id,
            exercise_index,
            exercise_id,
            exercise_name,
            notes
          FROM hevy.workout_exercises
          WHERE workout_id = ?
          ORDER BY exercise_index
          LIMIT %d
          ",
          limit
        ),
        params = list(trimws(workout_id))
      )
    )
  }

  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT
        workout_id,
        exercise_index,
        exercise_id,
        exercise_name,
        notes
      FROM hevy.workout_exercises
      ORDER BY workout_id, exercise_index
      LIMIT %d
      ",
      limit
    )
  )
}

#* List Hevy sets.
#* Requires bearer authentication.
#* @tag Hevy
#* @param workout_id Filter to one workout id (optional).
#* @param limit:int Maximum records to return (clamped to 1..10000).
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy sets returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Hevy curated sets table is unavailable.
#* @get /hevy/sets
function(res, workout_id = NULL, limit = 1000) {
  if (!hevy_table_exists("sets")) {
    return(missing_hevy_table(res, "sets"))
  }

  limit <- as.integer(limit)
  if (is.na(limit) || limit < 1) {
    limit <- 1000
  }
  if (limit > 10000) {
    limit <- 10000
  }

  if (!is.null(workout_id) && nzchar(trimws(workout_id))) {
    return(
      DBI::dbGetQuery(
        con,
        sprintf(
          "
          SELECT
            workout_id,
            exercise_index,
            set_index,
            set_type,
            reps,
            weight_kg,
            distance_m,
            duration_seconds,
            rpe,
            is_warmup,
            notes
          FROM hevy.sets
          WHERE workout_id = ?
          ORDER BY exercise_index, set_index
          LIMIT %d
          ",
          limit
        ),
        params = list(trimws(workout_id))
      )
    )
  }

  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT
        workout_id,
        exercise_index,
        set_index,
        set_type,
        reps,
        weight_kg,
        distance_m,
        duration_seconds,
        rpe,
        is_warmup,
        notes
      FROM hevy.sets
      ORDER BY workout_id, exercise_index, set_index
      LIMIT %d
      ",
      limit
    )
  )
}

#* List Hevy routines.
#* Requires bearer authentication.
#* @tag Hevy
#* @param limit:int Maximum records to return (clamped to 1..2000).
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy routines returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Hevy curated routines table is unavailable.
#* @get /hevy/routines
function(res, limit = 500) {
  if (!hevy_table_exists("routines")) {
    return(missing_hevy_table(res, "routines"))
  }

  limit <- as.integer(limit)
  if (is.na(limit) || limit < 1) {
    limit <- 500
  }
  if (limit > 2000) {
    limit <- 2000
  }

  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT
        routine_id,
        folder_id,
        routine_name,
        updated_at
      FROM hevy.routines
      ORDER BY updated_at DESC
      LIMIT %d
      ",
      limit
    )
  )
}
