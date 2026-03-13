#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(dplyr)
  library(purrr)
  library(tidyr)
  library(readr)
  library(stringr)
  library(arrow)
})

resolve_invocation_project_root <- function() {
  args_all <- commandArgs(trailingOnly = FALSE)
  file_arg <- args_all[grepl("^--file=", args_all)]

  if (length(file_arg) > 0) {
    script_path <- normalizePath(
      sub("^--file=", "", file_arg[[1]]),
      winslash = "/",
      mustWork = FALSE
    )
    return(dirname(dirname(script_path)))
  }

  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

load_project_env_if_needed <- function() {
  needed <- c("HEVY_API_KEY", "PERSONAL_DATA_DIR")
  missing <- needed[Sys.getenv(needed, unset = "") == ""]
  if (length(missing) == 0) {
    return(invisible(FALSE))
  }

  root <- resolve_invocation_project_root()
  candidates <- c(
    file.path(root, ".Renviron"),
    file.path(root, ".env.production")
  )

  for (f in candidates) {
    if (!file.exists(f)) {
      next
    }

    tryCatch(
      readRenviron(f),
      error = function(e) NULL
    )

    missing <- needed[Sys.getenv(needed, unset = "") == ""]
    if (length(missing) == 0) {
      break
    }
  }

  invisible(TRUE)
}

load_project_env_if_needed()

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0) y else x
}

hevy_base_url <- function() {
  Sys.getenv("HEVY_BASE_URL", unset = "https://api.hevyapp.com/v1")
}

hevy_api_key <- function() {
  key <- Sys.getenv("HEVY_API_KEY", unset = "")
  if (key == "") {
    stop("HEVY_API_KEY is not set.", call. = FALSE)
  }
  key
}

pd_root <- function() {
  path.expand(Sys.getenv("PERSONAL_DATA_DIR", unset = "~/personal-data"))
}

hevy_paths <- function(root = pd_root()) {
  list(
    root = root,
    raw = file.path(root, "raw", "hevy"),
    raw_workouts = file.path(root, "raw", "hevy", "workouts"),
    raw_events = file.path(root, "raw", "hevy", "events"),
    raw_routines = file.path(root, "raw", "hevy", "routines"),
    state = file.path(root, "raw", "hevy", "_state"),
    curated = file.path(root, "curated", "hevy"),
    logs = file.path(root, "logs"),
    db = file.path(root, "db", "warehouse.duckdb")
  )
}

ensure_hevy_dirs <- function(paths = hevy_paths()) {
  dirs <- unname(paths[c(
    "raw",
    "raw_workouts",
    "raw_events",
    "raw_routines",
    "state",
    "curated",
    "logs"
  )])

  walk(dirs, dir.create, recursive = TRUE, showWarnings = FALSE)
  invisible(paths)
}

hevy_log <- function(..., log_file = file.path(pd_root(), "logs", "hevy-sync.log")) {
  dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)
  msg <- paste0(
    format(Sys.time(), "%Y-%m-%d %H:%M:%S %z"),
    " ",
    paste(..., collapse = " ")
  )
  cat(msg, "\n", file = log_file, append = TRUE)
  message(msg)
}

write_raw_json <- function(x, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(
    x,
    path = path,
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null"
  )
  invisible(path)
}

state_file <- function(name, paths = hevy_paths()) {
  file.path(paths$state, name)
}

read_state <- function(name, default = NA_character_, paths = hevy_paths()) {
  f <- state_file(name, paths)
  if (!file.exists(f)) {
    return(default)
  }
  readr::read_file(f) |>
    stringr::str_trim()
}

write_state <- function(name, value, paths = hevy_paths()) {
  f <- state_file(name, paths)
  dir.create(dirname(f), recursive = TRUE, showWarnings = FALSE)
  writeLines(as.character(value), f)
  invisible(value)
}

read_positive_int_env <- function(name, default) {
  raw <- Sys.getenv(name, unset = as.character(default))
  val <- suppressWarnings(as.integer(raw))
  if (is.na(val) || val < 1L) {
    return(as.integer(default))
  }
  val
}

hevy_event_retention_days <- function() {
  read_positive_int_env("HEVY_EVENT_RETENTION_DAYS", default = 30L)
}

hevy_event_max_files <- function() {
  read_positive_int_env("HEVY_EVENT_MAX_FILES", default = 2000L)
}

cleanup_hevy_event_files <- function(
  paths = hevy_paths(),
  retention_days = hevy_event_retention_days(),
  max_files = hevy_event_max_files()
) {
  event_files <- list.files(paths$raw_events, pattern = "\\.json$", full.names = TRUE)
  if (length(event_files) == 0) {
    return(list(deleted_by_age = 0L, deleted_by_count = 0L, remaining = 0L))
  }

  now <- Sys.time()
  cutoff <- now - (as.numeric(retention_days) * 24 * 60 * 60)
  info <- file.info(event_files)
  info$file <- rownames(info)

  old_files <- info$file[!is.na(info$mtime) & info$mtime < cutoff]
  deleted_by_age <- 0L
  if (length(old_files) > 0) {
    deleted <- file.remove(old_files)
    deleted_by_age <- as.integer(sum(deleted, na.rm = TRUE))
  }

  remaining_files <- list.files(paths$raw_events, pattern = "\\.json$", full.names = TRUE)
  deleted_by_count <- 0L
  if (length(remaining_files) > max_files) {
    remaining_info <- file.info(remaining_files)
    remaining_info$file <- rownames(remaining_info)
    remaining_info <- remaining_info[order(remaining_info$mtime, decreasing = FALSE), , drop = FALSE]
    overflow <- length(remaining_files) - max_files
    to_delete <- head(remaining_info$file, overflow)

    deleted <- file.remove(to_delete)
    deleted_by_count <- as.integer(sum(deleted, na.rm = TRUE))
  }

  remaining <- length(list.files(paths$raw_events, pattern = "\\.json$", full.names = TRUE))

  list(
    deleted_by_age = deleted_by_age,
    deleted_by_count = deleted_by_count,
    remaining = remaining
  )
}

normalize_page_size <- function(page_size, max_page_size = 10L) {
  page_size <- suppressWarnings(as.integer(page_size))
  if (is.na(page_size) || page_size < 1L) {
    page_size <- 1L
  }
  min(page_size, max_page_size)
}

safe_num <- function(x) {
  suppressWarnings(as.numeric(x))
}

safe_chr <- function(x) {
  if (is.null(x) || length(x) == 0) NA_character_ else as.character(x)
}

safe_lgl <- function(x) {
  if (is.null(x) || length(x) == 0) NA else as.logical(x)
}

parse_time_utc <- function(x) {
  if (is.null(x) || length(x) == 0 || all(is.na(x))) {
    return(as.POSIXct(NA, origin = "1970-01-01", tz = "UTC"))
  }

  out <- suppressWarnings(as.POSIXct(x, tz = "UTC"))
  if (length(out) == 0) {
    as.POSIXct(NA, origin = "1970-01-01", tz = "UTC")
  } else {
    out
  }
}

extract_items <- function(x, candidates = c("items", "data", "workouts", "events", "routines", "folders")) {
  for (nm in candidates) {
    if (!is.null(x[[nm]]) && is.list(x[[nm]])) {
      return(x[[nm]])
    }
  }

  if (is.list(x) && length(x) > 0 && is.list(x[[1]])) {
    return(x)
  }

  list()
}

extract_total_pages <- function(x) {
  x$totalPages %||% x$pagination$totalPages %||% x$pages %||% NA_integer_
}

extract_page <- function(x) {
  x$page %||% x$pagination$page %||% NA_integer_
}

extract_workout_id <- function(x) {
  safe_chr(x$workoutId %||% x$workout_id %||% x$id)
}

extract_event_id <- function(x) {
  safe_chr(x$eventId %||% x$event_id %||% x$id)
}

extract_event_type <- function(x) {
  safe_chr(x$eventType %||% x$type %||% x$action)
}

extract_event_updated_at <- function(x) {
  safe_chr(x$updatedAt %||% x$createdAt %||% x$timestamp %||% x$occurredAt)
}

hevy_request <- function(path, query = list()) {
  request(paste0(hevy_base_url(), path)) |>
    req_headers(`api-key` = hevy_api_key()) |>
    req_url_query(!!!query)
}

hevy_get_json <- function(path, query = list(), max_tries = 3L) {
  req <- hevy_request(path, query)

  for (i in seq_len(max_tries)) {
    resp <- tryCatch(
      req |> req_perform(),
      error = function(e) e
    )

    if (!inherits(resp, "error")) {
      return(resp_body_json(resp, simplifyVector = FALSE))
    }

    if (i == max_tries) {
      stop(conditionMessage(resp), call. = FALSE)
    }

    Sys.sleep(i * 2)
  }

  stop("Unreachable", call. = FALSE)
}

fetch_workouts_count <- function() {
  hevy_get_json("/workouts/count")
}

fetch_workouts_page <- function(page = 1, page_size = 10) {
  page_size <- normalize_page_size(page_size)
  hevy_get_json("/workouts", query = list(page = page, pageSize = page_size))
}

fetch_workout_detail <- function(workout_id) {
  hevy_get_json(paste0("/workouts/", workout_id))
}

fetch_workout_events_page <- function(page = 1, page_size = 10, since = NULL) {
  page_size <- normalize_page_size(page_size)

  query <- list(
    page = page,
    pageSize = page_size
  )

  if (!is.null(since) && !is.na(since) && nzchar(since)) {
    query$since = since
  }

  hevy_get_json("/workouts/events", query = query)
}

fetch_routines_page <- function(page = 1, page_size = 10) {
  page_size <- normalize_page_size(page_size)
  hevy_get_json("/routines", query = list(page = page, pageSize = page_size))
}

collect_workout_ids_from_events <- function(events_resp) {
  items <- extract_items(events_resp, c("events", "items", "data"))
  ids <- map_chr(items, extract_workout_id)
  unique(ids[!is.na(ids) & ids != ""])
}

collect_events_tbl <- function(events_resp, fetched_at = Sys.time()) {
  items <- extract_items(events_resp, c("events", "items", "data"))

  if (length(items) == 0) {
    return(tibble(
      event_id = character(),
      workout_id = character(),
      event_type = character(),
      event_time_raw = character(),
      fetched_at = as.POSIXct(character(), tz = "UTC")
    ))
  }

  map_dfr(items, function(ev) {
    tibble(
      event_id = extract_event_id(ev),
      workout_id = extract_workout_id(ev),
      event_type = extract_event_type(ev),
      event_time_raw = extract_event_updated_at(ev),
      fetched_at = as.POSIXct(fetched_at, tz = "UTC")
    )
  })
}

backfill_workout_ids <- function(page_size = 10) {
  page_size <- normalize_page_size(page_size)

  count_resp <- fetch_workouts_count()

  hevy_log(
    "Raw workouts/count response:",
    jsonlite::toJSON(count_resp, auto_unbox = TRUE, null = "null")
  )

  total <- count_resp$count %||%
    count_resp$total %||%
    count_resp$workoutCount %||%
    count_resp$workout_count %||%
    0
  total <- suppressWarnings(as.integer(total))

  if (is.na(total)) {
    stop("Could not parse workouts/count response.", call. = FALSE)
  }

  if (total <= 0) {
    hevy_log("Backfill count is 0; skipping /workouts page fetch.")
    return(character())
  }

  pages <- ceiling(total / page_size)
  hevy_log("Backfill count:", total, "workouts across", pages, "pages")

  ids <- character()
  paths <- hevy_paths()

  for (page in seq_len(pages)) {
    resp <- fetch_workouts_page(page = page, page_size = page_size)

    write_raw_json(
      resp,
      file.path(paths$raw_workouts, sprintf("_page_%04d.json", page))
    )

    items <- extract_items(resp, c("workouts", "items", "data"))
    page_ids <- map_chr(items, extract_workout_id)
    ids <- c(ids, page_ids)

    hevy_log("Fetched workout list page", page, "items:", length(items))
    Sys.sleep(0.5)
  }

  unique(ids[!is.na(ids) & ids != ""])
}

fetch_and_store_workouts <- function(workout_ids, paths = hevy_paths()) {
  workout_ids <- unique(workout_ids[!is.na(workout_ids) & workout_ids != ""])

  if (length(workout_ids) == 0) {
    hevy_log("No workout ids to fetch")
    return(invisible(list()))
  }

  out <- vector("list", length(workout_ids))

  for (i in seq_along(workout_ids)) {
    id <- workout_ids[[i]]
    hevy_log("Fetching workout", i, "of", length(workout_ids), "id:", id)

    resp <- fetch_workout_detail(id)

    write_raw_json(
      resp,
      file.path(paths$raw_workouts, paste0(id, ".json"))
    )

    out[[i]] <- resp
    Sys.sleep(0.25)
  }

  invisible(out)
}

read_raw_workouts <- function(paths = hevy_paths()) {
  files <- list.files(
    paths$raw_workouts,
    pattern = "\\.json$",
    full.names = TRUE
  )

  files <- files[!grepl("/_page_", files)]

  if (length(files) == 0) {
    return(list())
  }

  map(files, ~ jsonlite::fromJSON(.x, simplifyVector = FALSE))
}

fetch_all_routines <- function(page_size = 10, paths = hevy_paths()) {
  page_size <- normalize_page_size(page_size)

  page <- 1L
  all_items <- list()

  repeat {
    resp <- fetch_routines_page(page = page, page_size = page_size)

    write_raw_json(
      resp,
      file.path(paths$raw_routines, sprintf("page_%04d.json", page))
    )

    items <- extract_items(resp, c("routines", "items", "data"))
    if (length(items) == 0) {
      break
    }

    all_items[[length(all_items) + 1L]] <- items
    hevy_log("Fetched routines page", page, "items:", length(items))

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

  all_items
}

normalize_workouts <- function(workouts) {
  if (length(workouts) == 0) {
    return(tibble(
      workout_id = character(),
      title = character(),
      description = character(),
      started_at = as.POSIXct(character(), tz = "UTC"),
      ended_at = as.POSIXct(character(), tz = "UTC"),
      duration_seconds = numeric(),
      created_at = as.POSIXct(character(), tz = "UTC"),
      updated_at = as.POSIXct(character(), tz = "UTC"),
      source = character()
    ))
  }

  map_dfr(workouts, function(w) {
    tibble(
      workout_id = safe_chr(w$id),
      title = safe_chr(w$title %||% w$name),
      description = safe_chr(w$description),
      started_at = parse_time_utc(w$startTime %||% w$start_time %||% w$startedAt),
      ended_at = parse_time_utc(w$endTime %||% w$end_time %||% w$endedAt),
      duration_seconds = safe_num(w$durationSeconds %||% w$duration %||% w$duration_seconds),
      created_at = parse_time_utc(w$createdAt %||% w$created_at),
      updated_at = parse_time_utc(w$updatedAt %||% w$updated_at),
      source = "hevy"
    )
  }) |>
    distinct(
      workout_id ,
      .keep_all = TRUE
    )
}

normalize_workout_exercises <- function(workouts) {
  if (length(workouts) == 0) {
    return(tibble(
      workout_id = character(),
      exercise_index = integer(),
      exercise_id = character(),
      exercise_name = character(),
      notes = character()
    ))
  }

  map_dfr(workouts, function(w) {
    exs <- w$exercises %||% list()
    if (length(exs) == 0) {
      return(NULL)
    }

    imap_dfr(exs, function(ex, idx) {
      tibble(
        workout_id = safe_chr(w$id),
        exercise_index = as.integer(idx),
        exercise_id = safe_chr(ex$id %||% ex$exerciseId %||% ex$exercise_id),
        exercise_name = safe_chr(ex$title %||% ex$name),
        notes = safe_chr(ex$notes)
      )
    })
  })
}

normalize_sets <- function(workouts) {
  if (length(workouts) == 0) {
    return(tibble(
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
      notes = character()
    ))
  }

  map_dfr(workouts, function(w) {
    exs <- w$exercises %||% list()
    if (length(exs) == 0) {
      return(NULL)
    }

    imap_dfr(exs, function(ex, ex_idx) {
      sts <- ex$sets %||% list()
      if (length(sts) == 0) {
        return(NULL)
      }

      imap_dfr(sts, function(st, set_idx) {
        tibble(
          workout_id = safe_chr(w$id),
          exercise_index = as.integer(ex_idx),
          set_index = as.integer(set_idx),
          set_type = safe_chr(st$type %||% st$setType),
          reps = safe_num(st$reps),
          weight_kg = safe_num(st$weightKg %||% st$weight_kg %||% st$weight),
          distance_m = safe_num(st$distanceMeters %||% st$distance_meters %||% st$distance),
          duration_seconds = safe_num(st$durationSeconds %||% st$duration_seconds),
          rpe = safe_num(st$rpe),
          is_warmup = safe_lgl(st$isWarmup %||% st$is_warmup),
          notes = safe_chr(st$notes)
        )
      })
    })
  })
}

normalize_routines <- function(routine_responses) {
  items <- flatten(routine_responses)

  if (length(items) == 0) {
    return(tibble(
      routine_id = character(),
      folder_id = character(),
      routine_name = character(),
      updated_at = as.POSIXct(character(), tz = "UTC")
    ))
  }

  map_dfr(items, function(r) {
    tibble(
      routine_id = safe_chr(r$id),
      folder_id = safe_chr(r$folderId %||% r$folder_id),
      routine_name = safe_chr(r$title %||% r$name),
      updated_at = parse_time_utc(r$updatedAt %||% r$updated_at)
    )
  }) |>
    distinct(
      routine_id ,
      .keep_all = TRUE
    )
}

write_curated_hevy <- function(
  workouts_tbl,
  exercises_tbl,
  sets_tbl,
  routines_tbl = NULL,
  paths = hevy_paths()
) {
  dir.create(paths$curated, recursive = TRUE, showWarnings = FALSE)

  arrow::write_parquet(workouts_tbl, file.path(paths$curated, "workouts.parquet"))
  arrow::write_parquet(exercises_tbl, file.path(paths$curated, "workout_exercises.parquet"))
  arrow::write_parquet(sets_tbl, file.path(paths$curated, "sets.parquet"))

  if (!is.null(routines_tbl)) {
    arrow::write_parquet(routines_tbl, file.path(paths$curated, "routines.parquet"))
  }

  invisible(TRUE)
}

register_hevy_views <- function(...) {
  stop(
    "register_hevy_views() is deprecated. Refresh DuckDB in a separate warehouse script.",
    call. = FALSE
  )
}
