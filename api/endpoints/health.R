register_health_endpoints <- function(pr) {
  pr$handle("GET", "/health", function() {
    list(status = "ok")
  })

  invisible(pr)
}
