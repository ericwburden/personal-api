DBI::dbExecute(
  con,
  "
CREATE TABLE IF NOT EXISTS snapshots (
  snapshot_id TEXT PRIMARY KEY,
  workspace TEXT NOT NULL,
  project TEXT NOT NULL,
  env TEXT NOT NULL,
  note TEXT,
  created_by TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  payload_json TEXT NOT NULL
)
"
)
