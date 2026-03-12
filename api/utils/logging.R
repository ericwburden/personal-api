log_event <- function(level, event, details = list()) {
  ts <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  detail_txt <- ""

  if (length(details) > 0) {
    kv <- vapply(
      names(details),
      function(k) paste0(k, "=", as.character(details[[k]])),
      character(1)
    )
    detail_txt <- paste(kv, collapse = " ")
  }

  prefix <- paste0("[", ts, "] [", toupper(level), "] ", event)
  if (nzchar(detail_txt)) {
    message(prefix, " ", detail_txt)
  } else {
    message(prefix)
  }

  invisible(TRUE)
}
