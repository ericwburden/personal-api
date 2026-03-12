library(DBI)
library(duckdb)

con <- dbConnect(
  duckdb::duckdb(),
  dbdir = "~/personal-data/db/warehouse.duckdb"
)

dbExecute(con, "
CREATE TABLE IF NOT EXISTS notes (
  id TEXT PRIMARY KEY,
  text TEXT,
  created_at TIMESTAMP
)
")

dbDisconnect(con, shutdown = TRUE)
