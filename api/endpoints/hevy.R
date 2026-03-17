#* Hevy API endpoints backed by curated DuckDB views.
#* Requires bearer authentication.

hevy_table_exists <- function(table) {
  DBI::dbExistsTable(con, DBI::Id(schema = "hevy", table = table))
}

missing_hevy_table <- function(res, table) {
  res$status <- 404
  list(error = paste0("Hevy table 'hevy.", table, "' is not available. Run Hevy sync first."))
}

read_hevy_state <- function(filename) {
  state_file <- file.path(personal_data_dir, "raw", "hevy", "_state", filename)
  if (!file.exists(state_file)) {
    return(NULL)
  }

  value <- trimws(readLines(state_file, warn = FALSE, n = 1))
  if (!nzchar(value)) {
    return(NULL)
  }

  value
}

read_hevy_last_refresh <- function() {
  has_warehouse_tables <- DBI::dbExistsTable(con, DBI::Id(schema = "warehouse", table = "tables"))
  if (!isTRUE(has_warehouse_tables)) {
    return(NULL)
  }

  out <- DBI::dbGetQuery(
    con,
    "
    SELECT MAX(last_refresh) AS last_refresh
    FROM warehouse.tables
    WHERE schema_name = 'hevy'
    "
  )

  value <- out$last_refresh[[1]]
  if (is.null(value) || is.na(value)) {
    return(NULL)
  }

  format(as.POSIXct(value, tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}

hevy_table_status <- function(table) {
  exists <- isTRUE(hevy_table_exists(table))
  if (!exists) {
    return(list(exists = FALSE, row_count = NULL))
  }

  out <- DBI::dbGetQuery(
    con,
    sprintf('SELECT COUNT(*) AS n FROM "hevy"."%s"', table)
  )

  list(
    exists = TRUE,
    row_count = as.integer(out$n[[1]])
  )
}

clamp_int <- function(x, default, min_value, max_value) {
  out <- suppressWarnings(as.integer(x))
  if (is.na(out)) {
    out <- default
  }
  out <- max(out, min_value)
  min(out, max_value)
}

normalize_sort_field <- function(sort_by, allowed, default) {
  value <- trimws(as.character(sort_by %||% default))
  if (!nzchar(value) || !(value %in% allowed)) {
    return(default)
  }
  value
}

normalize_sort_order <- function(order, default = "desc") {
  value <- tolower(trimws(as.character(order %||% default)))
  if (!(value %in% c("asc", "desc"))) {
    return(default)
  }
  value
}

normalize_grain <- function(grain, default = "week") {
  value <- tolower(trimws(as.character(grain %||% default)))
  if (!(value %in% c("day", "week", "month"))) {
    return(default)
  }
  value
}

normalize_utc_time <- function(x) {
  if (is.null(x)) {
    return(NULL)
  }

  value <- trimws(as.character(x))
  if (!nzchar(value)) {
    return(NULL)
  }

  parsed <- suppressWarnings(as.POSIXct(value, tz = "UTC"))
  if (is.na(parsed)) {
    return(NULL)
  }

  parsed
}

#* Return Hevy sync and table availability status.
#* Requires bearer authentication.
#* @tag Hevy
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy status returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @get /hevy/status
function() {
  list(
    last_backfill_at = read_hevy_state("last_backfill_at.txt"),
    last_sync_at = read_hevy_state("last_sync_at.txt"),
    last_refresh_utc = read_hevy_last_refresh(),
    tables = list(
      workouts = hevy_table_status("workouts"),
      workout_exercises = hevy_table_status("workout_exercises"),
      sets = hevy_table_status("sets"),
      routines = hevy_table_status("routines")
    )
  )
}

#* Return Hevy aggregate summary metrics for a date window.
#* Requires bearer authentication.
#* @tag Hevy
#* @param from Only include workouts at or after this UTC timestamp (optional).
#* @param to Only include workouts at or before this UTC timestamp (optional).
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy summary returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Required Hevy curated table is unavailable.
#* @get /hevy/summary
function(res, from = NULL, to = NULL) {
  if (!hevy_table_exists("workouts")) {
    return(missing_hevy_table(res, "workouts"))
  }
  if (!hevy_table_exists("sets")) {
    return(missing_hevy_table(res, "sets"))
  }

  from_time <- normalize_utc_time(from)
  to_time <- normalize_utc_time(to)

  where <- character()
  params <- list()

  if (!is.null(from_time)) {
    where <- c(where, "COALESCE(started_at, created_at) >= ?")
    params <- c(params, list(from_time))
  }
  if (!is.null(to_time)) {
    where <- c(where, "COALESCE(started_at, created_at) <= ?")
    params <- c(params, list(to_time))
  }

  where_sql <- if (length(where) == 0) "" else paste0("WHERE ", paste(where, collapse = " AND "))

  out <- DBI::dbGetQuery(
    con,
    sprintf(
      "
      WITH filtered_workouts AS (
        SELECT
          workout_id,
          CAST(COALESCE(started_at, created_at) AS TIMESTAMP) AS workout_time,
          duration_seconds
        FROM hevy.workouts
        %s
      ),
      workout_agg AS (
        SELECT
          COUNT(*) AS workouts,
          COUNT(DISTINCT CAST(workout_time AS DATE)) AS training_days,
          COALESCE(SUM(duration_seconds), 0) AS duration_seconds
        FROM filtered_workouts
      ),
      set_agg AS (
        SELECT
          COUNT(*) AS sets,
          COALESCE(SUM(COALESCE(s.reps, 0)), 0) AS reps,
          COALESCE(SUM(COALESCE(s.reps, 0) * COALESCE(s.weight_kg, 0)), 0) AS volume_kg
        FROM hevy.sets s
        INNER JOIN filtered_workouts w
          ON s.workout_id = w.workout_id
      )
      SELECT
        w.workouts,
        w.training_days,
        w.duration_seconds,
        s.sets,
        s.reps,
        s.volume_kg
      FROM workout_agg w
      CROSS JOIN set_agg s
      "
      ,
      where_sql
    ),
    params = params
  )

  list(
    from = if (is.null(from_time)) NULL else format(from_time, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    to = if (is.null(to_time)) NULL else format(to_time, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    workouts = as.integer(out$workouts[[1]]),
    training_days = as.integer(out$training_days[[1]]),
    duration_seconds = as.numeric(out$duration_seconds[[1]]),
    sets = as.integer(out$sets[[1]]),
    reps = as.numeric(out$reps[[1]]),
    volume_kg = as.numeric(out$volume_kg[[1]])
  )
}

#* Return Hevy aggregate metrics grouped by timeline period.
#* Requires bearer authentication.
#* @tag Hevy
#* @param grain Grouping granularity: `day`, `week`, or `month` (default `week`).
#* @param from Only include workouts at or after this UTC timestamp (optional).
#* @param to Only include workouts at or before this UTC timestamp (optional).
#* @param limit:int Maximum periods to return (clamped to 1..2000).
#* @param offset:int Periods to skip before returning results (clamped to 0..100000).
#* @param order Sort direction for period start: `asc` or `desc` (default `asc`).
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy timeline returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Required Hevy curated table is unavailable.
#* @get /hevy/timeline
function(res, grain = "week", from = NULL, to = NULL, limit = 200, offset = 0, order = "asc") {
  if (!hevy_table_exists("workouts")) {
    return(missing_hevy_table(res, "workouts"))
  }
  if (!hevy_table_exists("sets")) {
    return(missing_hevy_table(res, "sets"))
  }

  grain <- normalize_grain(grain, default = "week")
  limit <- clamp_int(limit, default = 200L, min_value = 1L, max_value = 2000L)
  offset <- clamp_int(offset, default = 0L, min_value = 0L, max_value = 100000L)
  order <- normalize_sort_order(order, default = "asc")
  from_time <- normalize_utc_time(from)
  to_time <- normalize_utc_time(to)

  where <- character()
  params <- list()

  if (!is.null(from_time)) {
    where <- c(where, "COALESCE(started_at, created_at) >= ?")
    params <- c(params, list(from_time))
  }
  if (!is.null(to_time)) {
    where <- c(where, "COALESCE(started_at, created_at) <= ?")
    params <- c(params, list(to_time))
  }

  where_sql <- if (length(where) == 0) "" else paste0("WHERE ", paste(where, collapse = " AND "))

  DBI::dbGetQuery(
    con,
    sprintf(
      "
      WITH filtered_workouts AS (
        SELECT
          workout_id,
          CAST(COALESCE(started_at, created_at) AS TIMESTAMP) AS workout_time,
          COALESCE(duration_seconds, 0) AS duration_seconds
        FROM hevy.workouts
        %s
      ),
      set_by_workout AS (
        SELECT
          workout_id,
          COUNT(*) AS sets,
          COALESCE(SUM(COALESCE(reps, 0)), 0) AS reps,
          COALESCE(SUM(COALESCE(reps, 0) * COALESCE(weight_kg, 0)), 0) AS volume_kg
        FROM hevy.sets
        GROUP BY workout_id
      )
      SELECT
        DATE_TRUNC('%s', fw.workout_time) AS period_start,
        COUNT(*) AS workouts,
        COUNT(DISTINCT CAST(fw.workout_time AS DATE)) AS training_days,
        COALESCE(SUM(fw.duration_seconds), 0) AS duration_seconds,
        COALESCE(SUM(COALESCE(sw.sets, 0)), 0) AS sets,
        COALESCE(SUM(COALESCE(sw.reps, 0)), 0) AS reps,
        COALESCE(SUM(COALESCE(sw.volume_kg, 0)), 0) AS volume_kg
      FROM filtered_workouts fw
      LEFT JOIN set_by_workout sw
        ON sw.workout_id = fw.workout_id
      GROUP BY period_start
      ORDER BY period_start %s
      LIMIT %d
      OFFSET %d
      ",
      where_sql,
      grain,
      order,
      limit,
      offset
    ),
    params = params
  )
}

#* List Hevy exercises with aggregate usage metrics.
#* Requires bearer authentication.
#* @tag Hevy
#* @param limit:int Maximum records to return (clamped to 1..2000).
#* @param offset:int Records to skip before returning results (clamped to 0..100000).
#* @param search Optional case-insensitive exercise name search.
#* @param from Only include workouts at or after this UTC timestamp (optional).
#* @param to Only include workouts at or before this UTC timestamp (optional).
#* @param sort_by Sort field: one of `exercise_name`, `workouts`, `sets`, `reps`, `volume_kg`, `last_performed`.
#* @param order Sort direction: `asc` or `desc` (default `desc`).
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy exercises returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Required Hevy curated table is unavailable.
#* @get /hevy/exercises
function(res, limit = 200, offset = 0, search = NULL, from = NULL, to = NULL, sort_by = "last_performed", order = "desc") {
  if (!hevy_table_exists("workouts")) {
    return(missing_hevy_table(res, "workouts"))
  }
  if (!hevy_table_exists("workout_exercises")) {
    return(missing_hevy_table(res, "workout_exercises"))
  }
  if (!hevy_table_exists("sets")) {
    return(missing_hevy_table(res, "sets"))
  }

  limit <- clamp_int(limit, default = 200L, min_value = 1L, max_value = 2000L)
  offset <- clamp_int(offset, default = 0L, min_value = 0L, max_value = 100000L)
  sort_by <- normalize_sort_field(
    sort_by,
    allowed = c("exercise_name", "workouts", "sets", "reps", "volume_kg", "last_performed"),
    default = "last_performed"
  )
  order <- normalize_sort_order(order, default = "desc")
  from_time <- normalize_utc_time(from)
  to_time <- normalize_utc_time(to)
  search_value <- trimws(as.character(search %||% ""))
  has_search <- nzchar(search_value)

  where <- character()
  params <- list()

  if (!is.null(from_time)) {
    where <- c(where, "COALESCE(started_at, created_at) >= ?")
    params <- c(params, list(from_time))
  }
  if (!is.null(to_time)) {
    where <- c(where, "COALESCE(started_at, created_at) <= ?")
    params <- c(params, list(to_time))
  }

  where_sql <- if (length(where) == 0) "" else paste0("WHERE ", paste(where, collapse = " AND "))

  if (has_search) {
    params <- c(params, list(paste0("%", search_value, "%")))
  }

  DBI::dbGetQuery(
    con,
    sprintf(
      "
      WITH filtered_workouts AS (
        SELECT
          workout_id,
          COALESCE(started_at, created_at) AS workout_time
        FROM hevy.workouts
        %s
      ),
      exercise_sets AS (
        SELECT
          we.exercise_id,
          COALESCE(NULLIF(TRIM(we.exercise_name), ''), we.exercise_id) AS exercise_name,
          we.workout_id,
          fw.workout_time,
          s.set_index,
          COALESCE(s.reps, 0) AS reps,
          COALESCE(s.weight_kg, 0) AS weight_kg
        FROM hevy.workout_exercises we
        INNER JOIN filtered_workouts fw
          ON we.workout_id = fw.workout_id
        LEFT JOIN hevy.sets s
          ON s.workout_id = we.workout_id
         AND s.exercise_index = we.exercise_index
        WHERE we.exercise_id IS NOT NULL
          AND TRIM(we.exercise_id) <> ''
      )
      SELECT
        exercise_id,
        exercise_name,
        COUNT(DISTINCT workout_id) AS workouts,
        COUNT(set_index) AS sets,
        COALESCE(SUM(reps), 0) AS reps,
        COALESCE(SUM(reps * weight_kg), 0) AS volume_kg,
        MAX(workout_time) AS last_performed
      FROM exercise_sets
      %s
      GROUP BY exercise_id, exercise_name
      ORDER BY %s %s, exercise_id ASC
      LIMIT %d
      OFFSET %d
      ",
      where_sql,
      if (has_search) "WHERE LOWER(exercise_name) LIKE LOWER(?)" else "",
      sort_by,
      order,
      limit,
      offset
    ),
    params = params
  )
}

#* Return per-workout history for a single Hevy exercise.
#* Requires bearer authentication.
#* @tag Hevy
#* @param exercise_id Exercise id from Hevy.
#* @param limit:int Maximum records to return (clamped to 1..2000).
#* @param offset:int Records to skip before returning results (clamped to 0..100000).
#* @param from Only include workouts at or after this UTC timestamp (optional).
#* @param to Only include workouts at or before this UTC timestamp (optional).
#* @param order Sort direction for workout time: `asc` or `desc` (default `desc`).
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy exercise history returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Exercise not found or required Hevy curated table is unavailable.
#* @get /hevy/exercises/<exercise_id>/history
function(res, exercise_id, limit = 200, offset = 0, from = NULL, to = NULL, order = "desc") {
  if (!hevy_table_exists("workouts")) {
    return(missing_hevy_table(res, "workouts"))
  }
  if (!hevy_table_exists("workout_exercises")) {
    return(missing_hevy_table(res, "workout_exercises"))
  }
  if (!hevy_table_exists("sets")) {
    return(missing_hevy_table(res, "sets"))
  }

  exercise_id <- trimws(as.character(exercise_id %||% ""))
  if (!nzchar(exercise_id)) {
    res$status <- 404
    return(list(error = "Exercise not found"))
  }

  exists <- DBI::dbGetQuery(
    con,
    "
    SELECT 1 AS found
    FROM hevy.workout_exercises
    WHERE exercise_id = ?
    LIMIT 1
    ",
    params = list(exercise_id)
  )

  if (nrow(exists) == 0) {
    res$status <- 404
    return(list(error = "Exercise not found"))
  }

  limit <- clamp_int(limit, default = 200L, min_value = 1L, max_value = 2000L)
  offset <- clamp_int(offset, default = 0L, min_value = 0L, max_value = 100000L)
  order <- normalize_sort_order(order, default = "desc")
  from_time <- normalize_utc_time(from)
  to_time <- normalize_utc_time(to)

  where <- c("we.exercise_id = ?")
  params <- list(exercise_id)

  if (!is.null(from_time)) {
    where <- c(where, "COALESCE(w.started_at, w.created_at) >= ?")
    params <- c(params, list(from_time))
  }
  if (!is.null(to_time)) {
    where <- c(where, "COALESCE(w.started_at, w.created_at) <= ?")
    params <- c(params, list(to_time))
  }

  where_sql <- paste(where, collapse = " AND ")

  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT
        we.exercise_id,
        COALESCE(NULLIF(TRIM(we.exercise_name), ''), we.exercise_id) AS exercise_name,
        w.workout_id,
        w.title AS workout_title,
        COALESCE(w.started_at, w.created_at) AS workout_time,
        COUNT(s.set_index) AS sets,
        COALESCE(SUM(COALESCE(s.reps, 0)), 0) AS reps,
        COALESCE(MAX(COALESCE(s.weight_kg, 0)), 0) AS top_weight_kg,
        COALESCE(SUM(COALESCE(s.reps, 0) * COALESCE(s.weight_kg, 0)), 0) AS volume_kg
      FROM hevy.workout_exercises we
      INNER JOIN hevy.workouts w
        ON we.workout_id = w.workout_id
      LEFT JOIN hevy.sets s
        ON s.workout_id = we.workout_id
       AND s.exercise_index = we.exercise_index
      WHERE %s
      GROUP BY
        we.exercise_id,
        COALESCE(NULLIF(TRIM(we.exercise_name), ''), we.exercise_id),
        w.workout_id,
        w.title,
        COALESCE(w.started_at, w.created_at)
      ORDER BY workout_time %s, w.workout_id ASC
      LIMIT %d
      OFFSET %d
      ",
      where_sql,
      order,
      limit,
      offset
    ),
    params = params
  )
}

#* List Hevy workouts ordered by most recent start time.
#* Requires bearer authentication.
#* @tag Hevy
#* @param limit:int Maximum records to return (clamped to 1..1000).
#* @param offset:int Records to skip before returning results (clamped to 0..100000).
#* @param from Only include workouts at or after this UTC timestamp (optional).
#* @param to Only include workouts at or before this UTC timestamp (optional).
#* @param sort_by Sort field: one of `started_at`, `created_at`, `updated_at`, `duration_seconds`, `title`.
#* @param order Sort direction: `asc` or `desc` (default `desc`).
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy workouts returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Hevy curated workouts table is unavailable.
#* @get /hevy/workouts
function(res, limit = 100, offset = 0, from = NULL, to = NULL, sort_by = "started_at", order = "desc") {
  if (!hevy_table_exists("workouts")) {
    return(missing_hevy_table(res, "workouts"))
  }

  limit <- clamp_int(limit, default = 100L, min_value = 1L, max_value = 1000L)
  offset <- clamp_int(offset, default = 0L, min_value = 0L, max_value = 100000L)
  sort_by <- normalize_sort_field(
    sort_by,
    allowed = c("started_at", "created_at", "updated_at", "duration_seconds", "title"),
    default = "started_at"
  )
  order <- normalize_sort_order(order, default = "desc")
  from_time <- normalize_utc_time(from)
  to_time <- normalize_utc_time(to)

  where <- character()
  params <- list()

  if (!is.null(from_time)) {
    where <- c(where, "COALESCE(started_at, created_at) >= ?")
    params <- c(params, list(from_time))
  }
  if (!is.null(to_time)) {
    where <- c(where, "COALESCE(started_at, created_at) <= ?")
    params <- c(params, list(to_time))
  }

  where_sql <- if (length(where) == 0) "" else paste0("WHERE ", paste(where, collapse = " AND "))

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
      %s
      ORDER BY %s %s
      LIMIT %d
      OFFSET %d
      ",
      where_sql,
      sort_by,
      order,
      limit
      ,
      offset
    ),
    params = params
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

#* Get one Hevy workout with related exercises and sets.
#* Requires bearer authentication.
#* @tag Hevy
#* @param workout_id Workout id from Hevy.
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Full Hevy workout payload returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Workout not found or Hevy workouts table unavailable.
#* @get /hevy/workouts/<workout_id>/full
function(res, workout_id) {
  if (!hevy_table_exists("workouts")) {
    return(missing_hevy_table(res, "workouts"))
  }

  workout <- DBI::dbGetQuery(
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

  if (nrow(workout) == 0) {
    res$status <- 404
    return(list(error = "Workout not found"))
  }

  exercises <- if (hevy_table_exists("workout_exercises")) {
    DBI::dbGetQuery(
      con,
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
      ",
      params = list(workout_id)
    )
  } else {
    data.frame(
      workout_id = character(),
      exercise_index = integer(),
      exercise_id = character(),
      exercise_name = character(),
      notes = character(),
      stringsAsFactors = FALSE
    )
  }

  sets <- if (hevy_table_exists("sets")) {
    DBI::dbGetQuery(
      con,
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
      ",
      params = list(workout_id)
    )
  } else {
    data.frame(
      workout_id = character(),
      exercise_index = integer(),
      set_index = integer(),
      set_type = character(),
      reps = numeric(),
      weight_kg = numeric(),
      distance_m = numeric(),
      duration_seconds = numeric(),
      rpe = numeric(),
      is_warmup = logical(),
      notes = character(),
      stringsAsFactors = FALSE
    )
  }

  list(
    workout = workout[1, , drop = FALSE],
    exercises = exercises,
    sets = sets
  )
}

#* List Hevy workout exercises.
#* Requires bearer authentication.
#* @tag Hevy
#* @param workout_id Filter to one workout id (optional).
#* @param limit:int Maximum records to return (clamped to 1..5000).
#* @param offset:int Records to skip before returning results (clamped to 0..100000).
#* @param sort_by Sort field: one of `workout_id`, `exercise_index`, `exercise_name`.
#* @param order Sort direction: `asc` or `desc` (default `asc`).
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy workout exercises returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Hevy curated workout_exercises table is unavailable.
#* @get /hevy/workout-exercises
function(res, workout_id = NULL, limit = 500, offset = 0, sort_by = "workout_id", order = "asc") {
  if (!hevy_table_exists("workout_exercises")) {
    return(missing_hevy_table(res, "workout_exercises"))
  }

  limit <- clamp_int(limit, default = 500L, min_value = 1L, max_value = 5000L)
  offset <- clamp_int(offset, default = 0L, min_value = 0L, max_value = 100000L)
  sort_by <- normalize_sort_field(
    sort_by,
    allowed = c("workout_id", "exercise_index", "exercise_name"),
    default = "workout_id"
  )
  order <- normalize_sort_order(order, default = "asc")

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
          ORDER BY %s %s, exercise_index ASC
          LIMIT %d
          OFFSET %d
          ",
          sort_by,
          order,
          limit
          ,
          offset
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
      ORDER BY %s %s, workout_id ASC, exercise_index ASC
      LIMIT %d
      OFFSET %d
      ",
      sort_by,
      order,
      limit
      ,
      offset
    )
  )
}

#* List Hevy sets.
#* Requires bearer authentication.
#* @tag Hevy
#* @param workout_id Filter to one workout id (optional).
#* @param limit:int Maximum records to return (clamped to 1..10000).
#* @param offset:int Records to skip before returning results (clamped to 0..100000).
#* @param sort_by Sort field: one of `workout_id`, `exercise_index`, `set_index`, `reps`, `weight_kg`.
#* @param order Sort direction: `asc` or `desc` (default `asc`).
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy sets returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Hevy curated sets table is unavailable.
#* @get /hevy/sets
function(res, workout_id = NULL, limit = 1000, offset = 0, sort_by = "workout_id", order = "asc") {
  if (!hevy_table_exists("sets")) {
    return(missing_hevy_table(res, "sets"))
  }

  limit <- clamp_int(limit, default = 1000L, min_value = 1L, max_value = 10000L)
  offset <- clamp_int(offset, default = 0L, min_value = 0L, max_value = 100000L)
  sort_by <- normalize_sort_field(
    sort_by,
    allowed = c("workout_id", "exercise_index", "set_index", "reps", "weight_kg"),
    default = "workout_id"
  )
  order <- normalize_sort_order(order, default = "asc")

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
          ORDER BY %s %s, exercise_index ASC, set_index ASC
          LIMIT %d
          OFFSET %d
          ",
          sort_by,
          order,
          limit
          ,
          offset
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
      ORDER BY %s %s, workout_id ASC, exercise_index ASC, set_index ASC
      LIMIT %d
      OFFSET %d
      ",
      sort_by,
      order,
      limit
      ,
      offset
    )
  )
}

#* List Hevy routines.
#* Requires bearer authentication.
#* @tag Hevy
#* @param limit:int Maximum records to return (clamped to 1..2000).
#* @param offset:int Records to skip before returning results (clamped to 0..100000).
#* @param sort_by Sort field: one of `updated_at`, `routine_name`, `routine_id`.
#* @param order Sort direction: `asc` or `desc` (default `desc`).
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Hevy routines returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @response 404 Hevy curated routines table is unavailable.
#* @get /hevy/routines
function(res, limit = 500, offset = 0, sort_by = "updated_at", order = "desc") {
  if (!hevy_table_exists("routines")) {
    return(missing_hevy_table(res, "routines"))
  }

  limit <- clamp_int(limit, default = 500L, min_value = 1L, max_value = 2000L)
  offset <- clamp_int(offset, default = 0L, min_value = 0L, max_value = 100000L)
  sort_by <- normalize_sort_field(
    sort_by,
    allowed = c("updated_at", "routine_name", "routine_id"),
    default = "updated_at"
  )
  order <- normalize_sort_order(order, default = "desc")

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
      ORDER BY %s %s, routine_id ASC
      LIMIT %d
      OFFSET %d
      ",
      sort_by,
      order,
      limit
      ,
      offset
    )
  )
}
