#* @apiTitle personal-api
#* @apiDescription API for notes and curated personal data warehouse metadata.
#* @apiTag System Endpoints for service availability and operational metadata.

#* Health check endpoint for uptime monitors and API clients
#* Returns service identity and current UTC time. No bearer token required.
#* @tag System
#* @serializer json list(auto_unbox = TRUE)
#* @response 200 Service is healthy and ready to receive requests.
#* @get /health
function() {
  list(
    status = "ok",
    service = "personal-api",
    timestamp_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  )
}
