DBI::dbExecute(
  con,
  "
CREATE TABLE IF NOT EXISTS contexts (
  workspace TEXT NOT NULL,
  project TEXT NOT NULL,
  env TEXT NOT NULL,
  context_id TEXT NOT NULL,
  title TEXT,
  content_json TEXT NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_by TEXT,
  published_version INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (workspace, project, env, context_id)
)
"
)

DBI::dbExecute(
  con,
  "
CREATE TABLE IF NOT EXISTS skills (
  workspace TEXT NOT NULL,
  project TEXT NOT NULL,
  env TEXT NOT NULL,
  skill_id TEXT NOT NULL,
  title TEXT,
  content_json TEXT NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_by TEXT,
  published_version INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (workspace, project, env, skill_id)
)
"
)

DBI::dbExecute(
  con,
  "
CREATE TABLE IF NOT EXISTS automations (
  workspace TEXT NOT NULL,
  project TEXT NOT NULL,
  env TEXT NOT NULL,
  automation_id TEXT NOT NULL,
  title TEXT,
  trigger_type TEXT,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  content_json TEXT NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_by TEXT,
  published_version INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (workspace, project, env, automation_id)
)
"
)

DBI::dbExecute(
  con,
  "
CREATE TABLE IF NOT EXISTS versions (
  workspace TEXT NOT NULL,
  project TEXT NOT NULL,
  env TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  title TEXT,
  content_json TEXT NOT NULL,
  tags_json TEXT,
  dependencies_json TEXT,
  source_updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by TEXT,
  change_note TEXT,
  PRIMARY KEY (workspace, project, env, object_type, object_id, version)
)
"
)

DBI::dbExecute(
  con,
  "
CREATE TABLE IF NOT EXISTS tags (
  workspace TEXT NOT NULL,
  project TEXT NOT NULL,
  env TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_id TEXT NOT NULL,
  tag TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (workspace, project, env, object_type, object_id, tag)
)
"
)

DBI::dbExecute(
  con,
  "
CREATE TABLE IF NOT EXISTS dependencies (
  workspace TEXT NOT NULL,
  project TEXT NOT NULL,
  env TEXT NOT NULL,
  source_type TEXT NOT NULL,
  source_id TEXT NOT NULL,
  target_type TEXT NOT NULL,
  target_id TEXT NOT NULL,
  required BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (workspace, project, env, source_type, source_id, target_type, target_id)
)
"
)
