router_env <- new.env(parent = globalenv())
pr <- source("api/api.R", local = router_env)$value

if (!inherits(pr, "pr")) {
  stop("api/api.R did not return a plumber router.", call. = FALSE)
}

pr$run(host = "127.0.0.1", port = 8000)
