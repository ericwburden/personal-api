DBI::dbExecute(
  con,
  "
CREATE TABLE IF NOT EXISTS automation_runs (
  run_id TEXT PRIMARY KEY,
  workspace TEXT NOT NULL,
  project TEXT NOT NULL,
  env TEXT NOT NULL,
  automation_id TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  finished_at TIMESTAMP,
  requested_by TEXT,
  dry_run BOOLEAN NOT NULL DEFAULT FALSE,
  input_json TEXT,
  result_json TEXT,
  error_text TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"
)

DBI::dbExecute(
  con,
  "
CREATE TABLE IF NOT EXISTS automation_run_logs (
  run_id TEXT NOT NULL,
  line_no INTEGER NOT NULL,
  level TEXT NOT NULL,
  message TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id, line_no)
)
"
)
