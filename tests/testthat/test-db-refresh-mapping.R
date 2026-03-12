test_that("derive_view_spec includes nested path segments in view name", {
  find_project_root <- function(start_dir = getwd()) {
    cur <- normalizePath(start_dir, winslash = "/", mustWork = TRUE)
    repeat {
      candidate <- file.path(cur, "api", "utils", "utils.R")
      if (file.exists(candidate)) {
        return(cur)
      }
      parent <- dirname(cur)
      if (identical(parent, cur)) {
        stop("Could not locate project root for tests.", call. = FALSE)
      }
      cur <- parent
    }
  }

  project_root <- find_project_root()
  env <- new.env(parent = globalenv())
  source(file.path(project_root, "api", "utils", "utils.R"), local = env)
  source(file.path(project_root, "api", "utils", "db-refresh.R"), local = env)

  curated_dir <- file.path(tempdir(), "curated-mapping")
  dir.create(file.path(curated_dir, "hevy", "raw"), recursive = TRUE, showWarnings = FALSE)
  file_path <- file.path(curated_dir, "hevy", "raw", "workouts.parquet")
  file.create(file_path)

  spec <- env$derive_view_spec(file_path, curated_dir)
  expect_equal(spec$schema, "hevy")
  expect_equal(spec$view, "raw_workouts")
  expect_equal(spec$object_name, "hevy.raw_workouts")
})

test_that("build_file_index fails on duplicate object name mappings", {
  find_project_root <- function(start_dir = getwd()) {
    cur <- normalizePath(start_dir, winslash = "/", mustWork = TRUE)
    repeat {
      candidate <- file.path(cur, "api", "utils", "utils.R")
      if (file.exists(candidate)) {
        return(cur)
      }
      parent <- dirname(cur)
      if (identical(parent, cur)) {
        stop("Could not locate project root for tests.", call. = FALSE)
      }
      cur <- parent
    }
  }

  project_root <- find_project_root()
  env <- new.env(parent = globalenv())
  source(file.path(project_root, "api", "utils", "utils.R"), local = env)
  source(file.path(project_root, "api", "utils", "db-refresh.R"), local = env)

  curated_dir <- file.path(tempdir(), "curated-collisions")
  dir.create(file.path(curated_dir, "hevy"), recursive = TRUE, showWarnings = FALSE)

  file_a <- file.path(curated_dir, "hevy", "one-two.parquet")
  file_b <- file.path(curated_dir, "hevy", "one_two.parquet")
  file.create(file_a)
  file.create(file_b)

  expect_error(
    env$build_file_index(c(file_a, file_b), curated_dir),
    "duplicate DuckDB object names"
  )
})
