DBI::dbExecute(
  con,
  "
CREATE TABLE IF NOT EXISTS notes (
  id TEXT PRIMARY KEY,
  text TEXT,
  created_at TIMESTAMP
)
"
)
