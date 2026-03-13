router_env <- new.env(parent = globalenv())
pr <- source("api/api.R", local = router_env)$value

if (is.null(pr) || !is.function(pr$run)) {
  stop("api/api.R did not return a runnable plumber router.", call. = FALSE)
}

pr$run(host = "127.0.0.1", port = 8000)
