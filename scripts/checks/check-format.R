target_dirs <- c("api", "scripts", "tests")

r_files <- unlist(
  lapply(target_dirs, function(d) {
    if (!dir.exists(d)) {
      return(character())
    }

    list.files(
      d,
      pattern = "\\.[Rr]$",
      recursive = TRUE,
      full.names = TRUE
    )
  }),
  use.names = FALSE
)

if (length(r_files) == 0) {
  message("No files checked for formatting.")
  quit(save = "no", status = 0)
}

issues <- data.frame(
  file = character(),
  issue = character(),
  line = integer(),
  stringsAsFactors = FALSE
)

for (f in r_files) {
  lines <- readLines(f, warn = FALSE)
  if (length(lines) == 0) {
    next
  }

  tab_lines <- which(grepl("\t", lines))
  if (length(tab_lines) > 0) {
    issues <- rbind(
      issues,
      data.frame(
        file = f,
        issue = "tab_character",
        line = tab_lines,
        stringsAsFactors = FALSE
      )
    )
  }

  trailing_ws_lines <- which(grepl("[ \t]+$", lines))
  if (length(trailing_ws_lines) > 0) {
    issues <- rbind(
      issues,
      data.frame(
        file = f,
        issue = "trailing_whitespace",
        line = trailing_ws_lines,
        stringsAsFactors = FALSE
      )
    )
  }
}

if (nrow(issues) > 0) {
  message("Formatting check failed. Fix tab/trailing whitespace issues:")
  print(issues, row.names = FALSE)
  quit(save = "no", status = 1)
}

message("Formatting checks passed for ", length(r_files), " files.")
