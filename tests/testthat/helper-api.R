start_test_api <- function(token = "test-token") {
  if (!requireNamespace("callr", quietly = TRUE) || !requireNamespace("httr2", quietly = TRUE)) {
    stop("Packages 'callr' and 'httr2' are required for API tests.", call. = FALSE)
  }

  project_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)
  data_dir <- file.path(tempdir(), paste0("personal-api-test-", as.integer(Sys.time())))

  dir.create(file.path(data_dir, "db"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(data_dir, "curated"), recursive = TRUE, showWarnings = FALSE)

  callr::r(
    function(project_dir, data_dir) {
      setwd(project_dir)
      Sys.setenv(PERSONAL_DATA_DIR = data_dir)
      source("scripts/0000-init-duckdb.R")
      invisible(TRUE)
    },
    args = list(project_dir = project_dir, data_dir = data_dir)
  )

  proc <- callr::r_bg(
    function(project_dir, data_dir, token) {
      setwd(project_dir)
      Sys.setenv(PERSONAL_DATA_DIR = data_dir, API_TOKEN = token)
      source("api/run-api.R")
    },
    args = list(project_dir = project_dir, data_dir = data_dir, token = token),
    supervise = TRUE,
    stdout = "|",
    stderr = "|"
  )

  base_url <- "http://127.0.0.1:8000"
  deadline <- Sys.time() + 15
  repeat {
    alive <- proc$is_alive()
    healthy <- FALSE

    if (alive) {
      healthy <- tryCatch({
        resp <- httr2::request(paste0(base_url, "/health")) |>
          httr2::req_error(is_error = function(resp) FALSE) |>
          httr2::req_timeout(2) |>
          httr2::req_perform()

        httr2::resp_status(resp) == 200
      }, error = function(e) FALSE)
    }

    if (healthy) {
      break
    }

    if (!alive || Sys.time() >= deadline) {
      out <- tryCatch(proc$read_all_output_lines(), error = function(e) character())
      err <- tryCatch(proc$read_all_error_lines(), error = function(e) character())
      proc$kill()
      stop(
        paste(
          c(
            "Timed out starting API for tests.",
            "STDOUT:",
            out,
            "STDERR:",
            err
          ),
          collapse = "\n"
        ),
        call. = FALSE
      )
    }

    Sys.sleep(0.2)
  }

  list(proc = proc, base_url = base_url, token = token)
}

stop_test_api <- function(api) {
  if (is.null(api) || is.null(api$proc)) {
    return(invisible(FALSE))
  }

  if (api$proc$is_alive()) {
    api$proc$kill()
    Sys.sleep(0.2)
  }

  invisible(TRUE)
}

perform_api_request <- function(api, method, path, token = NULL, body = NULL) {
  if (!requireNamespace("httr2", quietly = TRUE)) {
    stop("Package 'httr2' is required for API tests.", call. = FALSE)
  }

  req <- httr2::request(paste0(api$base_url, path)) |>
    httr2::req_method(method) |>
    httr2::req_error(is_error = function(resp) FALSE)

  if (!is.null(token)) {
    req <- req |>
      httr2::req_headers(Authorization = paste("Bearer", token))
  }

  if (!is.null(body)) {
    req <- req |>
      httr2::req_body_raw(body, type = "application/json")
  }

  httr2::req_perform(req)
}
