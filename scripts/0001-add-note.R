library(DBI)
library(duckdb)
library(uuid)

args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 1) {
  stop("Usage: Rscript 0001-add-note.R 'your note text'")
}

note_text <- args[1]

personal_data_dir <- path.expand(Sys.getenv("PERSONAL_DATA_DIR", unset = "~/personal-data"))
db_path <- file.path(personal_data_dir, "db", "warehouse.duckdb")

con <- dbConnect(
  duckdb::duckdb(),
  dbdir = db_path
)

dbExecute(
  con,
  "INSERT INTO notes (id, text, created_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
  params = list(UUIDgenerate(), note_text)
)

dbDisconnect(con, shutdown = TRUE)

message("Note inserted.")
