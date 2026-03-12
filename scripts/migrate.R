suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
})

resolve_script_dir <- function() {
  args_all <- commandArgs(trailingOnly = FALSE)
  file_arg <- args_all[grepl("^--file=", args_all)]

  if (length(file_arg) > 0) {
    return(dirname(normalizePath(sub("^--file=", "", file_arg[[1]]), winslash = "/", mustWork = FALSE)))
  }

  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

script_dir <- resolve_script_dir()
migrations_dir <- file.path(script_dir, "migrations")

personal_data_dir <- path.expand(Sys.getenv("PERSONAL_DATA_DIR", unset = "~/personal-data"))
db_path <- file.path(personal_data_dir, "db", "warehouse.duckdb")
dir.create(dirname(db_path), recursive = TRUE, showWarnings = FALSE)

drv <- duckdb::duckdb()
con <- DBI::dbConnect(
  drv,
  dbdir = db_path
)

DBI::dbExecute(
  con,
  "
CREATE TABLE IF NOT EXISTS schema_migrations (
  version TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"
)

if (!dir.exists(migrations_dir)) {
  message("No migrations directory found at ", migrations_dir)
} else {
  migration_files <- list.files(
    migrations_dir,
    pattern = "^[0-9]{4}-.+\\.[Rr]$",
    full.names = TRUE
  )
  migration_files <- sort(migration_files)

  if (length(migration_files) == 0) {
    message("No migration files found in ", migrations_dir)
  } else {
    applied_versions <- DBI::dbGetQuery(
      con,
      "SELECT version FROM schema_migrations"
    )$version
    if (is.null(applied_versions)) {
      applied_versions <- character()
    }

    for (migration_file in migration_files) {
      migration_name <- basename(migration_file)
      migration_version <- sub("-.*$", "", migration_name)

      if (migration_version %in% applied_versions) {
        message("Skipping migration ", migration_name, " (already applied)")
        next
      }

      message("Applying migration ", migration_name)

      DBI::dbWithTransaction(
        con,
        {
          migration_env <- new.env(parent = globalenv())
          migration_env$con <- con
          source(migration_file, local = migration_env)

          DBI::dbExecute(
            con,
            "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            params = list(migration_version, migration_name)
          )
        }
      )
    }
  }
}

message("Migrations complete.")
DBI::dbDisconnect(con, shutdown = TRUE)
