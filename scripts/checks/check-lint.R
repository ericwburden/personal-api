suppressPackageStartupMessages({
  library(lintr)
})

target_dirs <- c("api", "scripts", "tests")
lint_rules <- lintr::linters_with_defaults(
  line_length_linter = NULL,
  object_usage_linter = NULL,
  object_length_linter = NULL,
  commas_linter = NULL
)

all_lints <- do.call(
  c,
  lapply(target_dirs, function(d) {
    if (!dir.exists(d)) {
      return(list())
    }

    lintr::lint_dir(path = d, relative_path = TRUE, linters = lint_rules)
  })
)

if (length(all_lints) > 0) {
  print(all_lints)
  quit(save = "no", status = 1)
}

message("Lint checks passed.")
