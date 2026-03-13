read_env_string <- function(name, default = NULL, trim = TRUE) {
  unset <- if (is.null(default)) "" else as.character(default)
  value <- Sys.getenv(name, unset = unset)

  if (isTRUE(trim)) {
    value <- trimws(value)
  }

  value
}

resolve_personal_data_dir <- function() {
  path.expand(read_env_string("PERSONAL_DATA_DIR", default = "~/personal-data"))
}

resolve_api_token <- function(required = FALSE) {
  token <- read_env_string("API_TOKEN", default = "", trim = TRUE)

  if (isTRUE(required) && !nzchar(token)) {
    stop("API_TOKEN must be set before starting the API.", call. = FALSE)
  }

  token
}
