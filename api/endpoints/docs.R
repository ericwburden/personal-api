register_docs_endpoints <- function(pr) {
  swagger_ui_html <- paste(
    c(
      "<!doctype html>",
      "<html lang=\"en\">",
      "<head>",
      "  <meta charset=\"utf-8\" />",
      "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />",
      "  <title>personal-api docs</title>",
      "  <link rel=\"stylesheet\" href=\"https://unpkg.com/swagger-ui-dist@5/swagger-ui.css\" />",
      "  <style>",
      "    html { box-sizing: border-box; overflow-y: scroll; }",
      "    *, *:before, *:after { box-sizing: inherit; }",
      "    body { margin: 0; background: #fafafa; }",
      "  </style>",
      "</head>",
      "<body>",
      "  <div id=\"swagger-ui\"></div>",
      "  <script src=\"https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js\"></script>",
      "  <script src=\"https://unpkg.com/swagger-ui-dist@5/swagger-ui-standalone-preset.js\"></script>",
      "  <script>",
      "    window.ui = SwaggerUIBundle({",
      "      url: '/openapi.json',",
      "      dom_id: '#swagger-ui',",
      "      deepLinking: true,",
      "      presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],",
      "      layout: 'StandaloneLayout'",
      "    });",
      "  </script>",
      "</body>",
      "</html>"
    ),
    collapse = "\n"
  )

  pr$handle("GET", "/swagger", function(res) {
    res$setHeader("Content-Type", "text/html; charset=utf-8")
    res$setHeader("Cache-Control", "no-store")
    res$body <- swagger_ui_html
    res
  })

  invisible(pr)
}
