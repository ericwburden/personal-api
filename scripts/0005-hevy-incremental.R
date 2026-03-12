#!/usr/bin/env Rscript

resolve_hevy_core_path <- function() {
  args_all <- commandArgs(trailingOnly = FALSE)
  file_arg <- args_all[grepl("^--file=", args_all)]

  script_dir <- if (length(file_arg) > 0) {
    dirname(normalizePath(sub("^--file=", "", file_arg[[1]]), winslash = "/", mustWork = FALSE))
  } else {
    normalizePath(getwd(), winslash = "/", mustWork = FALSE)
  }

  candidates <- c(
    file.path(script_dir, "0003-hevy-core.R"),
    file.path(
      path.expand(Sys.getenv("PERSONAL_DATA_DIR", unset = "~/personal-data")),
      "scripts",
      "0003-hevy-core.R"
    )
  )

  existing <- candidates[file.exists(candidates)]
  if (length(existing) == 0) {
    stop(
      paste(
        "Could not locate 0003-hevy-core.R.",
        "Expected scripts/0003-hevy-core.R or PERSONAL_DATA_DIR/scripts/0003-hevy-core.R."
      ),
      call. = FALSE
    )
  }

  existing[[1]]
}

source(resolve_hevy_core_path())

paths <- hevy_paths()
ensure_hevy_dirs(paths)

hevy_log("Starting Hevy incremental sync")

since <- read_state("last_sync_at.txt", default = NA_character_, paths = paths)
if (is.na(since) || since == "") {
  stop(
    "Missing raw/hevy/_state/last_sync_at.txt. Run 0004-hevy-backfill.R first.",
    call. = FALSE
  )
}

is_delete_event <- function(event_type) {
  if (is.null(event_type) || length(event_type) == 0 || is.na(event_type)) {
    return(FALSE)
  }

  event_type <- tolower(trimws(as.character(event_type)))

  event_type %in%
    c(
      "delete",
      "deleted",
      "workout.deleted",
      "workout_deleted",
      "removed",
      "remove"
    )
}

delete_raw_workout_files <- function(workout_ids, paths) {
  workout_ids <- unique(workout_ids[!is.na(workout_ids) & nzchar(workout_ids)])

  if (length(workout_ids) == 0) {
    return(invisible(character()))
  }

  deleted_files <- character()

  for (id in workout_ids) {
    f <- file.path(paths$raw_workouts, paste0(id, ".json"))
    if (file.exists(f)) {
      ok <- file.remove(f)
      if (isTRUE(ok)) {
        deleted_files <- c(deleted_files, f)
        hevy_log("Deleted raw workout file for removed workout:", id)
      } else {
        hevy_log("Failed to delete raw workout file for removed workout:", id)
      }
    } else {
      hevy_log("Raw workout file already absent for removed workout:", id)
    }
  }

  invisible(deleted_files)
}

page <- 1L
page_size <- 10L
all_event_tbls <- list()
changed_ids <- character()
deleted_ids <- character()

repeat {
  resp <- fetch_workout_events_page(
    page = page,
    page_size = page_size,
    since = since
  )

  event_path <- file.path(
    paths$raw_events,
    paste0(
      format(Sys.time(), "%Y%m%dT%H%M%SZ", tz = "UTC"),
      sprintf("_page_%04d.json", page)
    )
  )

  write_raw_json(resp, event_path)

  event_tbl <- collect_events_tbl(resp)
  all_event_tbls[[length(all_event_tbls) + 1L]] <- event_tbl

  if (nrow(event_tbl) > 0) {
    event_tbl <- event_tbl |>
      mutate(
        event_type_norm = tolower(trimws(event_type)),
        is_deleted = vapply(event_type, is_delete_event, logical(1))
      )

    changed_ids <- unique(c(
      changed_ids,
      event_tbl$workout_id[!is.na(event_tbl$workout_id) & nzchar(event_tbl$workout_id)]
    ))

    deleted_ids <- unique(c(
      deleted_ids,
      event_tbl$workout_id[event_tbl$is_deleted & !is.na(event_tbl$workout_id) & nzchar(event_tbl$workout_id)]
    ))
  }

  items <- extract_items(resp, c("events", "items", "data"))
  hevy_log(
    "Fetched events page",
    page,
    "items:",
    length(items),
    "changed ids so far:",
    length(changed_ids),
    "deleted ids so far:",
    length(deleted_ids)
  )

  total_pages <- extract_total_pages(resp)
  if (!is.na(total_pages) && page >= total_pages) {
    break
  }
  if (length(items) < page_size && is.na(total_pages)) {
    break
  }

  page <- page + 1L
  Sys.sleep(0.25)
}

changed_ids <- unique(changed_ids)
deleted_ids <- unique(deleted_ids)

changed_non_deleted_ids <- setdiff(changed_ids, deleted_ids)

if (length(deleted_ids) > 0) {
  delete_raw_workout_files(deleted_ids, paths)
} else {
  hevy_log("No deleted workouts returned by events feed")
}

if (length(changed_non_deleted_ids) > 0) {
  fetch_and_store_workouts(changed_non_deleted_ids, paths = paths)
} else {
  hevy_log("No non-deleted changed workouts returned by events feed")
}

raw_workouts <- read_raw_workouts(paths)

workouts_tbl <- normalize_workouts(raw_workouts)
exercises_tbl <- normalize_workout_exercises(raw_workouts)
sets_tbl <- normalize_sets(raw_workouts)

routines_tbl <- tryCatch(
  {
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
  },
  error = function(e) {
    hevy_log("Routine normalization skipped:", e$message)
    NULL
  }
)

write_curated_hevy(
  workouts_tbl = workouts_tbl,
  exercises_tbl = exercises_tbl,
  sets_tbl = sets_tbl,
  routines_tbl = routines_tbl,
  paths = paths
)

events_all_tbl <- dplyr::bind_rows(all_event_tbls)
if (nrow(events_all_tbl) > 0) {
  events_all_tbl <- events_all_tbl |>
    mutate(
      is_deleted = vapply(event_type, is_delete_event, logical(1))
    )

  arrow::write_parquet(
    events_all_tbl,
    file.path(paths$curated, "workout_events_latest.parquet")
  )
}

write_state(
  "last_sync_at.txt",
  format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  paths
)

hevy_log(
  "Incremental sync complete.",
  "changed_workouts:",
  length(changed_ids),
  "deleted_workouts:",
  length(deleted_ids),
  "fetched_non_deleted_workouts:",
  length(changed_non_deleted_ids),
  "workouts:",
  nrow(workouts_tbl),
  "exercises:",
  nrow(exercises_tbl),
  "sets:",
  nrow(sets_tbl),
  "routines:",
  if (is.null(routines_tbl)) 0 else nrow(routines_tbl)
)
