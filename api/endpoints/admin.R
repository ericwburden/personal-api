#* Force a curated view refresh from Parquet mappings.
#* Requires bearer authentication.
#* @tag Warehouse
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Curated view refresh completed.
#* @response 401 Missing or invalid bearer token.
#* @post /admin/refresh-curated
function() {
  refresh_curated_views(force = TRUE)
}
