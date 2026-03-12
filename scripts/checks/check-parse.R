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
  message("No R files found for parse checks.")
  quit(save = "no", status = 0)
}

for (f in r_files) {
  parse(file = f)
}

message("Parse checks passed for ", length(r_files), " files.")
