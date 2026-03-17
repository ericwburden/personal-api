#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

source_file <- ".env.example"
target_file <- if (length(args) >= 1) args[[1]] else ".Renviron"

if (!file.exists(source_file)) {
  stop("Missing .env.example. Cannot initialize environment file.", call. = FALSE)
}

if (file.exists(target_file)) {
  stop(
    paste(
      target_file,
      "already exists. Edit it directly or remove it and rerun setup-env."
    ),
    call. = FALSE
  )
}

ok <- file.copy(source_file, target_file)
if (!isTRUE(ok)) {
  stop("Failed to copy .env.example to target file.", call. = FALSE)
}

message(
  "Created ",
  target_file,
  " from .env.example. Update API_TOKEN and HEVY_API_KEY before running scripts."
)
