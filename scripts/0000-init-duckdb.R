resolve_script_dir <- function() {
  args_all <- commandArgs(trailingOnly = FALSE)
  file_arg <- args_all[grepl("^--file=", args_all)]

  if (length(file_arg) > 0) {
    return(dirname(normalizePath(sub("^--file=", "", file_arg[[1]]), winslash = "/", mustWork = FALSE)))
  }

  source_path <- tryCatch(sys.frame(1)$ofile, error = function(e) NULL)
  if (!is.null(source_path) && nzchar(source_path)) {
    return(dirname(normalizePath(source_path, winslash = "/", mustWork = FALSE)))
  }

  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

script_dir <- resolve_script_dir()
migrate_script <- file.path(script_dir, "migrate.R")

if (!file.exists(migrate_script)) {
  stop("Could not find migrate.R next to 0000-init-duckdb.R", call. = FALSE)
}

source(migrate_script)
