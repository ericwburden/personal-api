register_auth_filter <- function(pr) {
  is_public_path <- function(path) {
    if (is.null(path) || !nzchar(path)) {
      return(FALSE)
    }

    normalized_path <- if (identical(path, "/")) path else sub("/+$", "", path)

    if (normalized_path %in% c("/health", "/swagger", "/openapi.json", "/swagger.json", "/__docs__")) {
      return(TRUE)
    }

    startsWith(normalized_path, "/__docs__")
  }

  pr$filter("auth", function(req, res) {
    if (is_public_path(req$PATH_INFO %||% "")) {
      return(plumber::forward())
    }

    if (!nzchar(api_token)) {
      res$status <- 503
      return(list(error = "API_TOKEN is not configured"))
    }

    auth <- trimws(req$HTTP_AUTHORIZATION %||% "")
    expected <- paste("Bearer", api_token)

    if (!identical(auth, expected)) {
      res$status <- 401
      return(list(error = "Unauthorized"))
    }

    tryCatch(
      maybe_refresh_curated_views(min_interval_secs = 60),
      error = function(e) {
        log_event(
          "error",
          "request_refresh_failed",
          details = list(
            method = req$REQUEST_METHOD %||% "UNKNOWN",
            path = req$PATH_INFO %||% "UNKNOWN",
            error = conditionMessage(e)
          )
        )
        NULL
      }
    )

    plumber::forward()
  })

  invisible(pr)
}
