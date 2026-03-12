library(DBI)
library(duckdb)

personal_data_dir <- path.expand(Sys.getenv("PERSONAL_DATA_DIR", unset = "~/personal-data"))
db_path <- file.path(personal_data_dir, "db", "warehouse.duckdb")
dir.create(dirname(db_path), recursive = TRUE, showWarnings = FALSE)

con <- dbConnect(
  duckdb::duckdb(),
  dbdir = db_path
)

dbExecute(
  con,
  "
CREATE TABLE IF NOT EXISTS notes (
  id TEXT PRIMARY KEY,
  text TEXT,
  created_at TIMESTAMP
)
"
)

dbDisconnect(con, shutdown = TRUE)
