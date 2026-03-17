start_test_api <- function(token = "test-token", port = NULL, setup_fn = NULL) {
  if (!requireNamespace("callr", quietly = TRUE) || !requireNamespace("httr2", quietly = TRUE)) {
    stop("Packages 'callr' and 'httr2' are required for API tests.", call. = FALSE)
  }

  find_project_dir <- function(start_dir = getwd()) {
    cur <- normalizePath(start_dir, winslash = "/", mustWork = TRUE)

    repeat {
      has_api <- file.exists(file.path(cur, "api", "run-api.R"))
      has_init <- file.exists(file.path(cur, "scripts", "0000-init-duckdb.R"))
      if (has_api && has_init) {
        return(cur)
      }

      parent <- dirname(cur)
      if (identical(parent, cur)) {
        break
      }
      cur <- parent
    }

    stop("Could not determine project root for tests.", call. = FALSE)
  }

  project_dir <- find_project_dir()
  data_dir <- file.path(tempdir(), paste0("personal-api-test-", as.integer(Sys.time())))
  api_script <- file.path(project_dir, "api", "api.R")

  dir.create(file.path(data_dir, "db"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(data_dir, "curated"), recursive = TRUE, showWarnings = FALSE)

  if (!file.exists(file.path(project_dir, "scripts", "0000-init-duckdb.R")) || !file.exists(api_script)) {
    stop("Could not locate API scripts for tests.", call. = FALSE)
  }

  callr::r(
    function(project_dir, data_dir) {
      setwd(project_dir)
      Sys.setenv(PERSONAL_DATA_DIR = data_dir)
      out <- system2("Rscript", c("scripts/0000-init-duckdb.R"), stdout = TRUE, stderr = TRUE)
      status <- attr(out, "status")
      if (is.null(status)) {
        status <- 0
      }
      if (status != 0) {
        stop(paste(out, collapse = "\n"), call. = FALSE)
      }
      invisible(TRUE)
    },
    args = list(project_dir = project_dir, data_dir = data_dir)
  )

  if (!is.null(setup_fn)) {
    setup_fn(data_dir = data_dir, project_dir = project_dir)
  }

  if (is.null(port)) {
    if (!requireNamespace("httpuv", quietly = TRUE)) {
      stop("Package 'httpuv' is required for API tests.", call. = FALSE)
    }
    port <- httpuv::randomPort()
  }
  port <- as.integer(port)
  if (is.na(port) || port < 1L || port > 65535L) {
    stop("`port` must be an integer between 1 and 65535.", call. = FALSE)
  }

  proc <- callr::r_bg(
    function(project_dir, api_script, data_dir, token, port) {
      setwd(project_dir)
      Sys.setenv(PERSONAL_DATA_DIR = data_dir, API_TOKEN = token)
      pr <- source(api_script)$value
      pr$run(host = "127.0.0.1", port = port)
    },
    args = list(project_dir = project_dir, api_script = api_script, data_dir = data_dir, token = token, port = port),
    supervise = TRUE,
    stdout = "|",
    stderr = "|"
  )

  base_url <- paste0("http://127.0.0.1:", port)
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
      if (alive) {
        proc$kill()
        Sys.sleep(0.2)
      }
      out <- tryCatch(proc$read_all_output_lines(), error = function(e) character())
      err <- tryCatch(proc$read_all_error_lines(), error = function(e) character())
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

  list(
    proc = proc,
    base_url = base_url,
    token = token,
    data_dir = data_dir,
    project_dir = project_dir
  )
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

perform_api_request <- function(api, method, path, token = NULL, body = NULL, content_type = "application/json") {
  if (!requireNamespace("httr2", quietly = TRUE)) {
    stop("Package 'httr2' is required for API tests.", call. = FALSE)
  }

  req <- httr2::request(paste0(api$base_url, path)) |>
    httr2::req_method(method) |>
    httr2::req_error(is_error = function(resp) FALSE) |>
    httr2::req_timeout(10)

  if (!is.null(token)) {
    req <- req |>
      httr2::req_headers(Authorization = paste("Bearer", token))
  }

  if (!is.null(body)) {
    req <- req |>
      httr2::req_body_raw(body, type = content_type)
  }

  httr2::req_perform(req)
}
