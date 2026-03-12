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

refresh_state <- new.env(parent = emptyenv())
refresh_state$last_checked_at <- as.POSIXct(NA)
refresh_state$file_index <- NULL
