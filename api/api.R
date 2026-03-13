library(plumber)

app <- new.env(parent = globalenv())

resolve_api_file <- function(...) {
  rel <- file.path(...)
  candidates <- c(
    file.path("api", rel),
    rel
  )

  existing <- candidates[file.exists(candidates)]
  if (length(existing) == 0) {
    stop(paste("Could not resolve API file:", rel), call. = FALSE)
  }

  existing[[1]]
}

source(resolve_api_file("utils", "config.R"), local = app)
source(resolve_api_file("utils", "utils.R"), local = app)
source(resolve_api_file("utils", "logging.R"), local = app)
source(resolve_api_file("utils", "db-refresh.R"), local = app)
source(resolve_api_file("utils", "auth-filter.R"), local = app)
source(resolve_api_file("utils", "error-handler.R"), local = app)

source(resolve_api_file("endpoints", "health.R"), local = app)
source(resolve_api_file("endpoints", "notes.R"), local = app)
source(resolve_api_file("endpoints", "tables.R"), local = app)
source(resolve_api_file("endpoints", "docs.R"), local = app)

pr <- plumber::pr()

app$register_error_handler(pr)
app$register_auth_filter(pr)
app$register_health_endpoints(pr)
app$register_notes_endpoints(pr)
app$register_table_endpoints(pr)
app$register_docs_endpoints(pr)

app$validate_startup_requirements(require_warehouse_tables = FALSE)
tryCatch(
  app$refresh_curated_views(force = TRUE),
  error = function(e) {
    app$log_event(
      "error",
      "startup_refresh_failed",
      details = list(error = conditionMessage(e))
    )
    NULL
  }
)

pr
