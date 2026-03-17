# personal-api

Small R-based personal data API backed by DuckDB, with a Hevy ingestion pipeline that writes curated Parquet files and exposes them as DuckDB views.

## What this repo contains

- `api/`: Plumber API server with token auth, notes endpoints, health, and table metadata endpoints.
- `scripts/`: one-off and scheduled scripts for DB initialization, notes insertion, heartbeat logging, Hevy backfill/incremental sync, and backup.

## Requirements

- R (4.2+ recommended)
- DuckDB and Plumber-compatible R runtime
- A writable personal data directory (default: `~/personal-data`)

Install required R packages:

```r
install.packages(c(
  "plumber", "DBI", "duckdb", "stringr", "jsonlite", "uuid",
  "httr2", "dplyr", "purrr", "tidyr", "readr", "arrow"
))
```

## Configuration

This repo uses `.env.example` as the env contract. Local secrets should live in `.Renviron` (ignored by Git), which you can initialize from the template:

```bash
Rscript scripts/utils/setup-env.R
```

Then edit `.Renviron` and set at least:

- `API_TOKEN` (required for API startup)
- `HEVY_API_KEY` (required for Hevy sync scripts)

Environment variables used by this project:

- `PERSONAL_DATA_DIR`: root directory for data/logs/db/scripts. Default: `~/personal-data`.
- `API_TOKEN`: bearer token required for all API routes except `/health`.
- `HEVY_API_KEY`: required for Hevy sync scripts.
- `HEVY_BASE_URL`: optional, defaults to `https://api.hevyapp.com/v1`.
- `HEVY_EVENT_RETENTION_DAYS`: optional, defaults to `30` (raw event cleanup age policy).
- `HEVY_EVENT_MAX_FILES`: optional, defaults to `2000` (raw event cleanup count cap).

Expected data layout under `PERSONAL_DATA_DIR`:

```text
<PERSONAL_DATA_DIR>/
  curated/
  raw/
    hevy/
      workouts/
      events/
      routines/
      _state/
  logs/
  db/
    warehouse.duckdb
```

`0004-hevy-backfill.R` and `0005-hevy-incremental.R` source `scripts/0003-hevy-core.R` from this repo by default. They also support a fallback copy at `<PERSONAL_DATA_DIR>/scripts/0003-hevy-core.R`.

## Quick start

1. Initialize your local environment file:

```bash
Rscript scripts/utils/setup-env.R
```

2. Initialize DuckDB `notes` table:

```bash
Rscript scripts/0000-init-duckdb.R
```

`0000-init-duckdb.R` runs the migration runner (`scripts/utils/migrate.R`) and records applied migrations in `schema_migrations`.

3. Build generated API routes:

```bash
Rscript scripts/utils/build-routes.R
```

Re-run this command whenever files in `api/endpoints/` change.

4. Start API:

```bash
# Linux/macOS
Rscript api/run-api.R

# PowerShell
Rscript api/run-api.R
```

API listens on `127.0.0.1:8000`.

## API reference

Auth:

- `GET /health` is unauthenticated.
- `GET /swagger/` is unauthenticated.
- `GET /__docs__/` is unauthenticated (Swagger UI).
- All other endpoints require `Authorization: Bearer <API_TOKEN>`.

Endpoints:

- `GET /health`
  - Returns service liveness metadata, including `status`, `service`, and `timestamp_utc`.
- `GET /swagger/`
  - Redirects to `/__docs__/` for Swagger UI compatibility.
- `GET /__docs__/`
  - Serves built-in Swagger UI and loads schema from `/openapi.json`.
- `GET /notes?limit=100`
  - Returns latest notes from DuckDB (`limit` clamped to `1..1000`).
- `POST /notes`
  - JSON body: `{ "text": "..." }`.
  - Inserts note with UUID and `CURRENT_TIMESTAMP`.
- `GET /hevy/workouts?limit=100`
  - Returns curated Hevy workouts.
  - Supports `offset`, `from`, `to`, `sort_by`, and `order`.
- `GET /hevy/status`
  - Returns Hevy sync status (`last_backfill_at`, `last_sync_at`, `last_refresh_utc`) and per-table availability/row counts.
- `GET /hevy/summary?from=<iso>&to=<iso>`
  - Returns aggregate metrics (`workouts`, `training_days`, `duration_seconds`, `sets`, `reps`, `volume_kg`) for the filtered window.
- `GET /hevy/timeline?grain=week`
  - Returns period rollups (`workouts`, `training_days`, `duration_seconds`, `sets`, `reps`, `volume_kg`).
  - Supports `grain` (`day|week|month`), `from`, `to`, `limit`, `offset`, and `order`.
- `GET /hevy/exercises?limit=200`
  - Returns exercise-level aggregates (`workouts`, `sets`, `reps`, `volume_kg`, `last_performed`).
  - Supports `offset`, `search`, `from`, `to`, `sort_by`, and `order`.
- `GET /hevy/exercises/<exercise_id>/history?limit=200`
  - Returns per-workout history for one exercise (`sets`, `reps`, `top_weight_kg`, `volume_kg`).
  - Supports `offset`, `from`, `to`, and `order`.
- `GET /hevy/workouts/<workout_id>`
  - Returns one curated Hevy workout by id.
- `GET /hevy/workouts/<workout_id>/full`
  - Returns one workout with related exercises and sets in a single payload.
- `GET /hevy/workout-exercises?workout_id=<id>&limit=500`
  - Returns curated workout exercises (optionally filtered by workout id).
  - Supports `offset`, `sort_by`, and `order`.
- `GET /hevy/sets?workout_id=<id>&limit=1000`
  - Returns curated workout sets (optionally filtered by workout id).
  - Supports `offset`, `sort_by`, and `order`.
- `GET /hevy/routines?limit=500`
  - Returns curated Hevy routines.
  - Supports `offset`, `sort_by`, and `order`.
- `GET /v1/contexts`, `GET /v1/skills`, `GET /v1/automations`
  - Lists workflow objects scoped by `workspace`, `project`, and `env`.
  - Supports `tag`, `q`, `sort_by`, `order`, `limit`, and `offset`.
- `PUT /v1/contexts/<context_id>`, `PUT /v1/skills/<skill_id>`, `PUT /v1/automations/<automation_id>`
  - Upserts draft object state from JSON body.
- `POST /v1/.../<id>/publish`, `GET /v1/.../<id>/versions`, `POST /v1/.../<id>/rollback`
  - Publishes immutable versions, lists versions, and restores prior versions.
- `PUT /v1/tags`, `GET /v1/tags`, `DELETE /v1/tags`
  - Manages tags for contexts, skills, and automations.
- `PUT /v1/dependencies`, `GET /v1/dependencies`, `DELETE /v1/dependencies`
  - Manages direct dependency edges between workflow objects.
- `GET /v1/catalog/search`, `GET /v1/catalog/tree`, `POST /v1/catalog/resolve`
  - Unified search/grouped retrieval and dependency-aware reference resolution.
- `GET /tables`
  - Returns table/view metadata from `warehouse.tables`.
- `POST /admin/refresh-curated`
  - Forces curated Parquet -> DuckDB view refresh.

## Curl examples

```bash
# Health (no auth required)
curl http://127.0.0.1:8000/health

# List notes
curl -H "Authorization: Bearer $API_TOKEN" \
  "http://127.0.0.1:8000/notes?limit=20"

# List Hevy workouts
curl -H "Authorization: Bearer $API_TOKEN" \
  "http://127.0.0.1:8000/hevy/workouts?limit=20&offset=0&sort_by=started_at&order=desc"

# Summarize Hevy activity in a date window
curl -H "Authorization: Bearer $API_TOKEN" \
  "http://127.0.0.1:8000/hevy/summary?from=2026-01-01T00:00:00Z&to=2026-12-31T23:59:59Z"

# Roll up Hevy activity by month
curl -H "Authorization: Bearer $API_TOKEN" \
  "http://127.0.0.1:8000/hevy/timeline?grain=month&from=2026-01-01T00:00:00Z&to=2026-12-31T23:59:59Z"

# List Hevy exercises with aggregate usage
curl -H "Authorization: Bearer $API_TOKEN" \
  "http://127.0.0.1:8000/hevy/exercises?limit=50&sort_by=last_performed&order=desc"

# Exercise history for a single exercise id
curl -H "Authorization: Bearer $API_TOKEN" \
  "http://127.0.0.1:8000/hevy/exercises/bench-press/history?limit=25&order=desc"

# Upsert a context draft
curl -X PUT http://127.0.0.1:8000/v1/contexts/session-context \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"title":"Session Context","content":{"summary":"alpha"},"updated_by":"eric"}'

# Publish a context version
curl -X POST http://127.0.0.1:8000/v1/contexts/session-context/publish \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"updated_by":"eric","change_note":"initial publish"}'

# Search unified workflow catalog
curl -H "Authorization: Bearer $API_TOKEN" \
  "http://127.0.0.1:8000/v1/catalog/search?q=session&type=context"

# Create note
curl -X POST http://127.0.0.1:8000/notes \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"text":"Remember to deload next week"}'

# List curated tables metadata
curl -H "Authorization: Bearer $API_TOKEN" \
  "http://127.0.0.1:8000/tables"

# Force curated refresh
curl -X POST http://127.0.0.1:8000/admin/refresh-curated \
  -H "Authorization: Bearer $API_TOKEN"
```

## Hevy ingestion workflow

Run order:

1. Backfill historical workouts and write curated Parquet:

```bash
Rscript scripts/0004-hevy-backfill.R
```

2. Incremental sync from event feed:

```bash
Rscript scripts/0005-hevy-incremental.R
```

Artifacts:

- Raw API payloads in `raw/hevy/**`.
- Curated Parquet files in `curated/hevy/*.parquet`.
- Sync state in `raw/hevy/_state/last_backfill_at.txt` and `last_sync_at.txt`.
- Logs in `logs/hevy-sync.log`.

## Utility scripts

- `scripts/0001-add-note.R`: insert a single note from CLI.
- `scripts/0002-heartbeat.R`: append timestamp line to `logs/heartbeat.log`.
- `scripts/utils/backup.sh`: tarball backup of `~/personal-data` into `~/backups`, deleting backups older than 7 days.

## Operational notes

- On API startup, curated Parquet views are refreshed once (`force = TRUE`).
- During authenticated requests, API performs a refresh check at most once per minute.
- Curated Parquet file paths are mapped to DuckDB views by folder/file name.

## Production operations

### Production branch deploy flow (VPS remote)

This workflow deploys by pushing `production` to a bare git repo on the VPS. The VPS hook checks out the latest code into your app directory and restarts the API service.

Branch policy:

- Feature branches merge into `main`.
- Only `main` is promoted into `production`.
- Every new commit on `production` must use a versioned subject: `vMAJOR.MINOR.PATCH: <message>`.

1. On your local machine, create and use the `production` branch (one-time):

```bash
git checkout -b production
```

2. Enable local hooks for production policy checks:

```bash
bash scripts/utils/setup-git-hooks.sh
```

3. On GitHub, protect `production`:

- Require pull requests.
- Add required status check: `production-policy / restrict-pr-source`.
- Restrict who can push directly to `production` (recommended).

4. On the VPS, install and configure the bare repo + hooks:

```bash
bash scripts/deploy/install-vps-bare-repo.sh \
  /srv/git/personal-api.git \
  /srv/personal-api \
  personal-api.service
```

5. On your local machine, add the VPS remote and push `production`:

```bash
bash scripts/deploy/add-vps-remote.sh \
  deploy@your-vps-host \
  /srv/git/personal-api.git \
  vps
```

6. Promote `main` into `production` with a versioned merge commit and tag:

```bash
bash scripts/release/commit-production.sh 1.0.0 "initial production deploy"
```

This script:

- pulls latest `origin/main` and `origin/production`
- merges `main` into `production` with commit subject `v1.0.0: initial production deploy`
- Annotated tag: `v1.0.0`

7. Push to both remotes:

```bash
git push origin production --follow-tags
git push vps production --follow-tags
```

8. Overwrite VPS user crontab to point scheduled jobs at the new deployment path:

```bash
bash scripts/deploy/overwrite-vps-crontab.sh \
  eric@api.ericburden.dev \
  /home/eric/personal-api \
  /home/eric/personal-data
```

When `production` is pushed to `vps`, the VPS `post-receive` hook:

- checks out the branch into `/srv/personal-api`
- optionally runs `renv::restore()` (off by default)
- runs `sudo systemctl restart personal-api.service`

If you need to enable `renv::restore()` on each deploy:

```bash
git --git-dir=/srv/git/personal-api.git config deploy.runRenvRestore true
```

If deploy user needs passwordless service restart, add a sudoers rule:

```text
<deploy-user> ALL=(root) NOPASSWD:/bin/systemctl restart personal-api.service
```

### CI environments

- Keep `.env.example` updated when env vars change.
- In GitHub Actions, inject secrets with repository/environment secrets and the `env:` block when needed.
- CI runs `scripts/checks/check-env.R` to enforce the template contract.

### Service environments

Example `cron` entries:

```cron
# Hevy incremental sync every 15 minutes
*/15 * * * * cd /path/to/personal-api && Rscript scripts/0005-hevy-incremental.R >> ~/personal-data/logs/cron-hevy-incremental.log 2>&1

# Daily backup at 03:30
30 3 * * * cd /path/to/personal-api && bash scripts/utils/backup.sh >> ~/personal-data/logs/cron-backup.log 2>&1
```

Example `systemd` service (`/etc/systemd/system/personal-api.service`):

```ini
[Unit]
Description=personal-api plumber service
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/personal-api
EnvironmentFile=/etc/personal-api/personal-api.env
ExecStart=/usr/bin/Rscript api/run-api.R
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Example `/etc/personal-api/personal-api.env`:

```env
API_TOKEN=replace-with-strong-token
PERSONAL_DATA_DIR=/home/your-user/personal-data
HEVY_API_KEY=replace-with-hevy-key
HEVY_BASE_URL=https://api.hevyapp.com/v1
HEVY_EVENT_RETENTION_DAYS=30
HEVY_EVENT_MAX_FILES=2000
```

Backup restore example:

```bash
mkdir -p ~/restore-personal-data
tar -xzf ~/backups/personal-data-YYYY-MM-DD-HHMM.tar.gz -C ~/restore-personal-data
```
