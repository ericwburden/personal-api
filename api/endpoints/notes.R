suppressPackageStartupMessages({
  library(jsonlite)
  library(uuid)
})

register_notes_endpoints <- function(pr) {
  pr$handle("GET", "/notes", function(limit = 100) {
    limit <- as.integer(limit)
    if (is.na(limit) || limit < 1) limit <- 100
    if (limit > 1000) limit <- 1000

    DBI::dbGetQuery(
      con,
      sprintf(
        "
        SELECT id, text, created_at
        FROM notes
        ORDER BY created_at DESC
        LIMIT %d
        ",
        limit
      )
    )
  })

  pr$handle("POST", "/notes", function(req, res) {
    body <- jsonlite::fromJSON(req$postBody)

    if (is.null(body$text) || !nzchar(body$text)) {
      res$status <- 400
      return(list(error = "Field 'text' is required"))
    }

    id <- uuid::UUIDgenerate()

    DBI::dbExecute(
      con,
      "INSERT INTO notes (id, text, created_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
      params = list(id, body$text)
    )

    list(id = id, text = body$text, status = "created")
  })

  invisible(pr)
}
