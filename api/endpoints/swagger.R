#* Redirect legacy Swagger endpoint to built-in docs UI.
#* Public endpoint. Redirect target: `/__docs__/`.
#* @tag System
#* @serializer text
#* @response 302 Redirect to the built-in Swagger UI endpoint.
#* @get /swagger/
function(res) {
  res$status <- 302
  res$setHeader("Location", "/__docs__/")
  "Redirecting to /__docs__/"
}
