library(plumber)

app <- new.env(parent = globalenv())

source("api/utils/config.R", local = app)
source("api/utils/utils.R", local = app)
source("api/utils/logging.R", local = app)
source("api/utils/db-refresh.R", local = app)
source("api/utils/auth-filter.R", local = app)
source("api/utils/error-handler.R", local = app)

source("api/endpoints/health.R", local = app)
source("api/endpoints/notes.R", local = app)
source("api/endpoints/tables.R", local = app)

pr <- plumber::pr()

app$register_error_handler(pr)
app$register_auth_filter(pr)
app$register_health_endpoints(pr)
app$register_notes_endpoints(pr)
app$register_table_endpoints(pr)

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
