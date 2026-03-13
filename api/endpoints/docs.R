register_docs_endpoints <- function(pr) {
  pr$handle("GET", "/swagger", function(res) {
    res$status <- 302L
    res$setHeader("Location", "/__docs__/")
    list(message = "Redirecting to Swagger UI")
  })

  invisible(pr)
}
