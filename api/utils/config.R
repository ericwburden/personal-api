suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
})

resolve_utils_file <- function(filename) {
  candidates <- c(
    file.path("api", "utils", filename),
    file.path("utils", filename),
    filename
  )

  existing <- candidates[file.exists(candidates)]
  if (length(existing) == 0) {
    stop(paste("Could not resolve utils file:", filename), call. = FALSE)
  }

  existing[[1]]
}

source(resolve_utils_file("env-config.R"), local = TRUE)

personal_data_dir <- resolve_personal_data_dir()
db_path <- file.path(personal_data_dir, "db", "warehouse.duckdb")
curated_dir <- file.path(personal_data_dir, "curated")
api_token <- resolve_api_token(required = FALSE)

con <- DBI::dbConnect(
  duckdb::duckdb(),
  dbdir = db_path,
  read_only = FALSE
)

validate_startup_requirements <- function(require_warehouse_tables = FALSE) {
  resolve_api_token(required = TRUE)

  if (!DBI::dbIsValid(con)) {
    stop("DuckDB connection is invalid at startup.", call. = FALSE)
  }

  if (!DBI::dbExistsTable(con, "notes")) {
    stop(
      "Required table 'notes' is missing. Run scripts/0000-init-duckdb.R first.",
      call. = FALSE
    )
  }

  if (isTRUE(require_warehouse_tables)) {
    has_warehouse_tables <- DBI::dbExistsTable(
      con,
      DBI::Id(schema = "warehouse", table = "tables")
    )

    if (!isTRUE(has_warehouse_tables)) {
      stop(
        "Required table 'warehouse.tables' is missing after refresh.",
        call. = FALSE
      )
    }
  }

  invisible(TRUE)
}

refresh_state <- new.env(parent = emptyenv())
refresh_state$last_checked_at <- as.POSIXct(NA)
refresh_state$file_index <- NULL
