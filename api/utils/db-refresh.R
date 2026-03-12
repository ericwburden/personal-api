suppressPackageStartupMessages({
  library(DBI)
})

list_curated_parquet_files <- function(curated_dir) {
  if (!dir.exists(curated_dir)) {
    return(character())
  }

  list.files(
    curated_dir,
    pattern = "\\.parquet$",
    recursive = TRUE,
    full.names = TRUE
  )
}

derive_view_spec <- function(file, curated_dir) {
  rel <- sub(
    paste0("^", normalizePath(curated_dir, winslash = "/"), "/"),
    "",
    normalizePath(file, winslash = "/")
  )

  rel_no_ext <- sub("\\.parquet$", "", rel, ignore.case = TRUE)
  parts <- strsplit(rel_no_ext, "/")[[1]]

  if (length(parts) == 1) {
    schema <- "data"
    view <- parts[1]
  } else {
    schema <- parts[1]
    view <- parts[length(parts)]
  }

  list(
    schema = sanitize_identifier(schema),
    view = sanitize_identifier(view),
    file = normalizePath(file, winslash = "/", mustWork = FALSE),
    rel_path = rel,
    object_name = paste0(sanitize_identifier(schema), ".", sanitize_identifier(view))
  )
}

build_file_index <- function(files, curated_dir) {
  if (length(files) == 0) {
    return(data.frame(
      file = character(),
      rel_path = character(),
      schema = character(),
      view = character(),
      object_name = character(),
      mtime = numeric(),
      stringsAsFactors = FALSE
    ))
  }

  do.call(
    rbind,
    lapply(files, function(f) {
      spec <- derive_view_spec(f, curated_dir)
      info <- file.info(f)

      data.frame(
        file = spec$file,
        rel_path = spec$rel_path,
        schema = spec$schema,
        view = spec$view,
        object_name = spec$object_name,
        mtime = as.numeric(info$mtime),
        stringsAsFactors = FALSE
      )
    })
  )
}

create_or_replace_view <- function(con, spec) {
  DBI::dbExecute(
    con,
    sprintf('CREATE SCHEMA IF NOT EXISTS "%s"', spec$schema)
  )

  DBI::dbExecute(
    con,
    sprintf(
      paste(
        'CREATE OR REPLACE VIEW "%s"."%s" AS',
        "SELECT * FROM read_parquet('%s')"
      ),
      spec$schema,
      spec$view,
      spec$file
    )
  )

  invisible(spec$object_name)
}

drop_missing_views <- function(con, old_index, new_index) {
  old_names <- old_index$object_name %||% character()
  new_names <- new_index$object_name %||% character()
  to_drop <- setdiff(old_names, new_names)

  for (nm in to_drop) {
    parts <- strsplit(nm, ".", fixed = TRUE)[[1]]
    if (length(parts) != 2) {
      next
    }

    DBI::dbExecute(
      con,
      sprintf('DROP VIEW IF EXISTS "%s"."%s"', parts[1], parts[2])
    )
  }

  invisible(to_drop)
}

read_view_row_count <- function(con, schema, view) {
  out <- DBI::dbGetQuery(
    con,
    sprintf('SELECT COUNT(*) AS n FROM "%s"."%s"', schema, view)
  )
  as.numeric(out$n[[1]])
}

write_warehouse_tables <- function(con, file_index, refreshed_at = Sys.time()) {
  DBI::dbExecute(con, 'CREATE SCHEMA IF NOT EXISTS "warehouse"')

  if (nrow(file_index) == 0) {
    empty_tbl <- data.frame(
      schema_name = character(),
      table_name = character(),
      object_name = character(),
      source_path = character(),
      relative_path = character(),
      row_count = numeric(),
      last_refresh = as.POSIXct(character()),
      stringsAsFactors = FALSE
    )

    DBI::dbWriteTable(
      con,
      DBI::Id(schema = "warehouse", table = "tables"),
      empty_tbl,
      overwrite = TRUE
    )

    return(invisible(TRUE))
  }

  meta <- do.call(
    rbind,
    lapply(seq_len(nrow(file_index)), function(i) {
      data.frame(
        schema_name = file_index$schema[[i]],
        table_name = file_index$view[[i]],
        object_name = file_index$object_name[[i]],
        source_path = file_index$file[[i]],
        relative_path = file_index$rel_path[[i]],
        row_count = read_view_row_count(con, file_index$schema[[i]], file_index$view[[i]]),
        last_refresh = as.POSIXct(refreshed_at, tz = "UTC"),
        stringsAsFactors = FALSE
      )
    })
  )

  DBI::dbWriteTable(
    con,
    DBI::Id(schema = "warehouse", table = "tables"),
    meta,
    overwrite = TRUE
  )

  invisible(TRUE)
}

refresh_curated_views <- function(force = FALSE) {
  files <- list_curated_parquet_files(curated_dir)
  new_index <- build_file_index(files, curated_dir)
  old_index <- refresh_state$file_index

  changed <- force

  if (is.null(old_index)) {
    changed <- TRUE
  } else if (nrow(old_index) != nrow(new_index)) {
    changed <- TRUE
  } else {
    old_cmp <- old_index[order(old_index$file), c("file", "mtime"), drop = FALSE]
    new_cmp <- new_index[order(new_index$file), c("file", "mtime"), drop = FALSE]
    changed <- !identical(old_cmp, new_cmp)
  }

  if (!changed) {
    return(list(
      changed = FALSE,
      refreshed = 0L,
      dropped = 0L,
      files = nrow(new_index)
    ))
  }

  refreshed <- 0L

  for (i in seq_len(nrow(new_index))) {
    spec <- list(
      schema = new_index$schema[[i]],
      view = new_index$view[[i]],
      file = new_index$file[[i]],
      rel_path = new_index$rel_path[[i]],
      object_name = new_index$object_name[[i]]
    )

    create_or_replace_view(con, spec)
    refreshed <- refreshed + 1L
  }

  dropped <- 0L
  if (!is.null(old_index)) {
    dropped_names <- drop_missing_views(con, old_index, new_index)
    dropped <- length(dropped_names)
  }

  write_warehouse_tables(con, new_index, refreshed_at = Sys.time())

  refresh_state$file_index <- new_index
  refresh_state$last_checked_at <- Sys.time()

  list(
    changed = TRUE,
    refreshed = refreshed,
    dropped = dropped,
    files = nrow(new_index)
  )
}

maybe_refresh_curated_views <- function(min_interval_secs = 60) {
  now <- Sys.time()
  last_checked <- refresh_state$last_checked_at

  if (is.na(last_checked) || difftime(now, last_checked, units = "secs") >= min_interval_secs) {
    out <- refresh_curated_views(force = FALSE)
    refresh_state$last_checked_at <- now
    return(out)
  }

  list(
    changed = FALSE,
    refreshed = 0L,
    dropped = 0L,
    files = if (is.null(refresh_state$file_index)) 0L else nrow(refresh_state$file_index)
  )
}
