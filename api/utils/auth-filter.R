register_auth_filter <- function(pr) {
  pr$filter("auth", function(req, res) {
    if (req$PATH_INFO == "/health") {
      return(plumber::forward())
    }

    auth <- req$HTTP_AUTHORIZATION %||% ""
    expected <- paste("Bearer", api_token)

    if (!identical(auth, expected)) {
      res$status <- 401
      return(list(error = "Unauthorized"))
    }

    try(maybe_refresh_curated_views(min_interval_secs = 60), silent = TRUE)

    plumber::forward()
  })

  invisible(pr)
}
