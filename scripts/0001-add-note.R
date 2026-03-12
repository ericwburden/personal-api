library(DBI)
library(duckdb)
library(uuid)

args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 1) {
  stop("Usage: Rscript 0001-add-note.R 'your note text'")
}

note_text <- args[1]

con <- dbConnect(
  duckdb::duckdb(),
  dbdir = "~/personal-data/db/warehouse.duckdb"
)

dbExecute(
  con,
  "INSERT INTO notes (id, text, created_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
  params = list(UUIDgenerate(), note_text)
)

dbDisconnect(con, shutdown = TRUE)

message("Note inserted.")
