test_that("API startup fails when API_TOKEN is missing", {
  testthat::skip_if_not_installed("callr")

  find_project_root <- function(start_dir = getwd()) {
    cur <- normalizePath(start_dir, winslash = "/", mustWork = TRUE)
    repeat {
      candidate <- file.path(cur, "api", "api.R")
      if (file.exists(candidate)) {
        return(cur)
      }
      parent <- dirname(cur)
      if (identical(parent, cur)) {
        stop("Could not locate project root for tests.", call. = FALSE)
      }
      cur <- parent
    }
  }

  project_root <- find_project_root()
  data_dir <- file.path(tempdir(), paste0("personal-api-startup-", as.integer(Sys.time())))
  dir.create(file.path(data_dir, "db"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(data_dir, "curated"), recursive = TRUE, showWarnings = FALSE)

  testthat::expect_error(
    callr::r(
      function(project_root, data_dir) {
        setwd(project_root)
        Sys.setenv(PERSONAL_DATA_DIR = data_dir)
        Sys.unsetenv("API_TOKEN")
        out <- system2("Rscript", c("scripts/0000-init-duckdb.R"), stdout = TRUE, stderr = TRUE)
        status <- attr(out, "status")
        if (is.null(status)) {
          status <- 0
        }
        if (status != 0) {
          stop(paste(out, collapse = "\n"), call. = FALSE)
        }
        source("api/api.R")
      },
      args = list(project_root = project_root, data_dir = data_dir)
    ),
    "API_TOKEN must be set"
  )
})
