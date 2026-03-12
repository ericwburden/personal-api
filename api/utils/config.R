suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
})

personal_data_dir <- path.expand(Sys.getenv("PERSONAL_DATA_DIR", "~/personal-data"))
db_path <- file.path(personal_data_dir, "db", "warehouse.duckdb")
curated_dir <- file.path(personal_data_dir, "curated")
api_token <- Sys.getenv("API_TOKEN")
api_token <- trimws(api_token)

con <- DBI::dbConnect(
  duckdb::duckdb(),
  dbdir = db_path,
  read_only = FALSE
)

validate_startup_requirements <- function(require_warehouse_tables = FALSE) {
  if (!nzchar(api_token)) {
    stop("API_TOKEN must be set before starting the API.", call. = FALSE)
  }

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
