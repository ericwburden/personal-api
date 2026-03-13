expected_vars <- c(
  "API_TOKEN",
  "PERSONAL_DATA_DIR",
  "HEVY_API_KEY",
  "HEVY_BASE_URL",
  "HEVY_EVENT_RETENTION_DAYS",
  "HEVY_EVENT_MAX_FILES"
)

required_default_vars <- c(
  "PERSONAL_DATA_DIR",
  "HEVY_BASE_URL",
  "HEVY_EVENT_RETENTION_DAYS",
  "HEVY_EVENT_MAX_FILES"
)

env_file <- ".env.example"
if (!file.exists(env_file)) {
  stop("Missing .env.example.", call. = FALSE)
}

lines <- readLines(env_file, warn = FALSE)
lines <- trimws(lines)
lines <- lines[nzchar(lines)]
lines <- lines[!grepl("^#", lines)]

if (length(lines) == 0) {
  stop(".env.example does not define any variables.", call. = FALSE)
}

bad_lines <- lines[!grepl("^[A-Za-z_][A-Za-z0-9_]*=.*$", lines)]
if (length(bad_lines) > 0) {
  stop(
    paste(
      "Invalid .env.example entries:",
      paste(bad_lines, collapse = "; ")
    ),
    call. = FALSE
  )
}

keys <- sub("=.*$", "", lines)
vals <- sub("^[A-Za-z_][A-Za-z0-9_]*=", "", lines)

dupes <- unique(keys[duplicated(keys)])
if (length(dupes) > 0) {
  stop(
    paste(
      "Duplicate keys in .env.example:",
      paste(dupes, collapse = ", ")
    ),
    call. = FALSE
  )
}

missing_vars <- setdiff(expected_vars, keys)
if (length(missing_vars) > 0) {
  stop(
    paste(
      "Missing expected env vars in .env.example:",
      paste(missing_vars, collapse = ", ")
    ),
    call. = FALSE
  )
}

required_default_idx <- match(required_default_vars, keys)
blank_defaults <- required_default_vars[!nzchar(vals[required_default_idx])]
if (length(blank_defaults) > 0) {
  stop(
    paste(
      "Expected non-empty defaults in .env.example for:",
      paste(blank_defaults, collapse = ", ")
    ),
    call. = FALSE
  )
}

retention_days <- suppressWarnings(as.integer(vals[match("HEVY_EVENT_RETENTION_DAYS", keys)]))
max_files <- suppressWarnings(as.integer(vals[match("HEVY_EVENT_MAX_FILES", keys)]))

if (is.na(retention_days) || retention_days < 1L) {
  stop("HEVY_EVENT_RETENTION_DAYS must be a positive integer in .env.example.", call. = FALSE)
}

if (is.na(max_files) || max_files < 1L) {
  stop("HEVY_EVENT_MAX_FILES must be a positive integer in .env.example.", call. = FALSE)
}

base_url <- vals[match("HEVY_BASE_URL", keys)]
if (!grepl("^https?://", base_url)) {
  stop("HEVY_BASE_URL in .env.example must start with http:// or https://.", call. = FALSE)
}

message("Environment template check passed.")
