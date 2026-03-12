register_auth_filter <- function(pr) {
  pr$filter("auth", function(req, res) {
    if (req$PATH_INFO == "/health") {
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
