#!/usr/bin/env Rscript

source(
  file.path(
    Sys.getenv("PERSONAL_DATA_DIR", unset = "~/personal-data"),
    "scripts",
    "0003-hevy-core.R"
  )
)

paths <- hevy_paths()
ensure_hevy_dirs(paths)

hevy_log("Starting Hevy backfill")

workout_ids <- backfill_workout_ids(page_size = 10)
hevy_log("Unique workout ids found:", length(workout_ids))

if (length(workout_ids) == 0) {
  hevy_log("No workouts returned by API; writing empty curated tables.")

  workouts_tbl <- normalize_workouts(list())
  exercises_tbl <- normalize_workout_exercises(list())
  sets_tbl <- normalize_sets(list())

  routines_tbl <- tryCatch({
    routine_files <- list.files(paths$raw_routines, pattern = "\\.json$", full.names = TRUE)
    if (length(routine_files) == 0) {
      NULL
    } else {
      routines_raw <- purrr::map(
        routine_files,
        ~ jsonlite::fromJSON(.x, simplifyVector = FALSE)
      ) |>
        purrr::map(~ extract_items(.x, c("routines", "items", "data")))

      normalize_routines(routines_raw)
    }
  }, error = function(e) {
    hevy_log("Routine normalization skipped:", e$message)
    NULL
  })

  write_curated_hevy(
    workouts_tbl = workouts_tbl,
    exercises_tbl = exercises_tbl,
    sets_tbl = sets_tbl,
    routines_tbl = routines_tbl,
    paths = paths
  )

  write_state(
    "last_backfill_at.txt",
    format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    paths
  )
  write_state(
    "last_sync_at.txt",
    format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    paths
  )

  hevy_log(
    "Backfill complete with zero workouts.",
    "routines:", if (is.null(routines_tbl)) 0 else nrow(routines_tbl)
  )

  quit(save = "no", status = 0)
}

fetch_and_store_workouts(workout_ids)

raw_workouts <- read_raw_workouts(paths)

workouts_tbl <- normalize_workouts(raw_workouts)
exercises_tbl <- normalize_workout_exercises(raw_workouts)
sets_tbl <- normalize_sets(raw_workouts)

routines_tbl <- tryCatch({
  routines_raw <- fetch_all_routines(page_size = 10)
  normalize_routines(routines_raw)
}, error = function(e) {
  hevy_log("Routine fetch skipped or failed:", e$message)

  routine_files <- list.files(paths$raw_routines, pattern = "\\.json$", full.names = TRUE)
  if (length(routine_files) == 0) {
    return(NULL)
  }

  routines_raw <- purrr::map(
    routine_files,
    ~ jsonlite::fromJSON(.x, simplifyVector = FALSE)
  ) |>
    purrr::map(~ extract_items(.x, c("routines", "items", "data")))

  normalize_routines(routines_raw)
})

write_curated_hevy(
  workouts_tbl = workouts_tbl,
  exercises_tbl = exercises_tbl,
  sets_tbl = sets_tbl,
  routines_tbl = routines_tbl,
  paths = paths
)

write_state(
  "last_backfill_at.txt",
  format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  paths
)
write_state(
  "last_sync_at.txt",
  format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  paths
)

hevy_log(
  "Backfill complete.",
  "workouts:", nrow(workouts_tbl),
  "exercises:", nrow(exercises_tbl),
  "sets:", nrow(sets_tbl),
  "routines:", if (is.null(routines_tbl)) 0 else nrow(routines_tbl)
)
