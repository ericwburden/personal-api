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

1. Initialize DuckDB `notes` table:

```bash
Rscript scripts/0000-init-duckdb.R
```

`0000-init-duckdb.R` runs the migration runner (`scripts/migrate.R`) and records applied migrations in `schema_migrations`.

2. Start API:

```bash
# Linux/macOS
export API_TOKEN="your-token"
Rscript api/run-api.R

# PowerShell
$env:API_TOKEN="your-token"
Rscript api/run-api.R
```

API listens on `127.0.0.1:8000`.

## API reference

Auth:

- `GET /health` is unauthenticated.
- All other endpoints require `Authorization: Bearer <API_TOKEN>`.

Endpoints:

- `GET /health`
  - Returns `{ "status": "ok" }`.
- `GET /notes?limit=100`
  - Returns latest notes from DuckDB (`limit` clamped to `1..1000`).
- `POST /notes`
  - JSON body: `{ "text": "..." }`.
  - Inserts note with UUID and `CURRENT_TIMESTAMP`.
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
- `scripts/backup.sh`: tarball backup of `~/personal-data` into `~/backups`, deleting backups older than 7 days.

## Operational notes

- On API startup, curated Parquet views are refreshed once (`force = TRUE`).
- During authenticated requests, API performs a refresh check at most once per minute.
- Curated Parquet file paths are mapped to DuckDB views by folder/file name.

## Production operations

Example `cron` entries:

```cron
# Hevy incremental sync every 15 minutes
*/15 * * * * cd /path/to/personal-api && Rscript scripts/0005-hevy-incremental.R >> ~/personal-data/logs/cron-hevy-incremental.log 2>&1

# Daily backup at 03:30
30 3 * * * cd /path/to/personal-api && bash scripts/backup.sh >> ~/personal-data/logs/cron-backup.log 2>&1
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
Environment=API_TOKEN=your-token
Environment=PERSONAL_DATA_DIR=/home/your-user/personal-data
ExecStart=/usr/bin/Rscript api/run-api.R
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Backup restore example:

```bash
mkdir -p ~/restore-personal-data
tar -xzf ~/backups/personal-data-YYYY-MM-DD-HHMM.tar.gz -C ~/restore-personal-data
```
