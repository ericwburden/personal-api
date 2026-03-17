#* List curated warehouse table metadata.
#* Requires bearer authentication.
#* @tag Warehouse
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Table metadata returned successfully.
#* @response 401 Missing or invalid bearer token.
#* @get /tables
function() {
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
}
