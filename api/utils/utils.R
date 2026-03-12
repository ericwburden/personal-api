suppressPackageStartupMessages({
  library(stringr)
})

`%||%` <- \(x, y) if (is.null(x) || length(x) == 0) y else x

sanitize_identifier <- function(x) {
  x |>
    tolower() |>
    str_replace_all("[^a-z0-9]+", "_") |>
    str_replace_all("^_+|_+$", "")
}
