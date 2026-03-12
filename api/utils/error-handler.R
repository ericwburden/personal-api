register_error_handler <- function(pr) {
  pr$setErrorHandler(function(req, res, err) {
    res$status <- 500

    log_event(
      "error",
      "request_error",
      details = list(
        method = req$REQUEST_METHOD %||% "UNKNOWN",
        path = req$PATH_INFO %||% "UNKNOWN",
        error = conditionMessage(err)
      )
    )

    list(error = "Internal server error")
  })

  invisible(pr)
}
