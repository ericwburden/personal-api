library(plumber)

app <- new.env(parent = globalenv())

source("api/utils/config.R", local = app)
source("api/utils/utils.R", local = app)
source("api/utils/db-refresh.R", local = app)
source("api/utils/auth-filter.R", local = app)

source("api/endpoints/health.R", local = app)
source("api/endpoints/notes.R", local = app)
source("api/endpoints/tables.R", local = app)

pr <- plumber::pr()

app$register_auth_filter(pr)
app$register_health_endpoints(pr)
app$register_notes_endpoints(pr)
app$register_table_endpoints(pr)

try(app$refresh_curated_views(force = TRUE), silent = TRUE)

pr
