register_table_endpoints <- function(pr) {
  pr$handle("GET", "/tables", function() {
    DBI::dbGetQuery(
      con,
      "
      SELECT
        schema_name,
        table_name,
        object_name,
        source_path,
        relative_path,
        row_count,
        last_refresh
      FROM warehouse.tables
      ORDER BY schema_name, table_name
      "
    )
  })

  pr$handle("POST", "/admin/refresh-curated", function() {
    refresh_curated_views(force = TRUE)
  })

  invisible(pr)
}
