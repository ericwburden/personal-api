#* Workflow diff endpoint to compare source and target scopes.
#* Requires bearer authentication.

wf_diff_ref_object <- function(item) {
  list(
    object_type = as.character(item$object_type %||% ""),
    object_id = as.character(item$object_id %||% "")
  )
}

wf_diff_ref_tag <- function(item) {
  list(
    object_type = as.character(item$object_type %||% ""),
    object_id = as.character(item$object_id %||% ""),
    tag = as.character(item$tag %||% "")
  )
}

wf_diff_ref_dependency <- function(item) {
  list(
    source_type = as.character(item$source_type %||% ""),
    source_id = as.character(item$source_id %||% ""),
    target_type = as.character(item$target_type %||% ""),
    target_id = as.character(item$target_id %||% "")
  )
}

wf_diff_ref_version <- function(item) {
  list(
    object_type = as.character(item$object_type %||% ""),
    object_id = as.character(item$object_id %||% ""),
    version = as.integer(item$version %||% 0L)
  )
}

wf_diff_key_object <- function(item) {
  paste0(as.character(item$object_type %||% ""), ":", as.character(item$object_id %||% ""))
}

wf_diff_key_tag <- function(item) {
  paste0(
    as.character(item$object_type %||% ""), ":",
    as.character(item$object_id %||% ""), ":",
    as.character(item$tag %||% "")
  )
}

wf_diff_key_dependency <- function(item) {
  paste0(
    as.character(item$source_type %||% ""), ":",
    as.character(item$source_id %||% ""), "->",
    as.character(item$target_type %||% ""), ":",
    as.character(item$target_id %||% "")
  )
}

wf_diff_key_version <- function(item) {
  paste0(
    as.character(item$object_type %||% ""), ":",
    as.character(item$object_id %||% ""), ":v",
    as.integer(item$version %||% 0L)
  )
}

wf_diff_signature <- function(item) {
  wf_json_encode(item %||% list())
}

wf_diff_index <- function(items, key_fn, signature_fn) {
  out <- list()
  values <- items %||% list()

  if (!is.list(values) || length(values) == 0L) {
    return(out)
  }

  for (item in values) {
    if (!is.list(item)) {
      next
    }

    key <- trimws(as.character(key_fn(item) %||% ""))
    if (!nzchar(key)) {
      next
    }

    out[[key]] <- list(item = item, signature = as.character(signature_fn(item) %||% ""))
  }

  out
}

wf_diff_trim <- function(items, limit) {
  if (length(items) <= limit) {
    return(items)
  }
  utils::head(items, n = limit)
}

wf_diff_compare <- function(source_items, target_items, key_fn, ref_fn, signature_fn, limit = 200L, include_unchanged = FALSE) {
  source_index <- wf_diff_index(source_items, key_fn, signature_fn)
  target_index <- wf_diff_index(target_items, key_fn, signature_fn)

  source_keys <- names(source_index)
  target_keys <- names(target_index)
  all_keys <- sort(unique(c(source_keys, target_keys)))

  source_only <- list()
  target_only <- list()
  changed <- list()
  unchanged <- list()

  for (key in all_keys) {
    source_row <- source_index[[key]]
    target_row <- target_index[[key]]

    if (is.null(source_row) && !is.null(target_row)) {
      target_only[[length(target_only) + 1L]] <- ref_fn(target_row$item)
      next
    }

    if (!is.null(source_row) && is.null(target_row)) {
      source_only[[length(source_only) + 1L]] <- ref_fn(source_row$item)
      next
    }

    if (!identical(as.character(source_row$signature), as.character(target_row$signature))) {
      changed[[length(changed) + 1L]] <- list(
        ref = ref_fn(source_row$item),
        source = source_row$item,
        target = target_row$item
      )
    } else if (isTRUE(include_unchanged)) {
      unchanged[[length(unchanged) + 1L]] <- ref_fn(source_row$item)
    }
  }

  list(
    counts = list(
      source_only = as.integer(length(source_only)),
      target_only = as.integer(length(target_only)),
      changed = as.integer(length(changed)),
      unchanged = as.integer(length(unchanged))
    ),
    source_only = wf_diff_trim(source_only, limit),
    target_only = wf_diff_trim(target_only, limit),
    changed = wf_diff_trim(changed, limit),
    unchanged = if (isTRUE(include_unchanged)) wf_diff_trim(unchanged, limit) else list(),
    truncated = list(
      source_only = length(source_only) > limit,
      target_only = length(target_only) > limit,
      changed = length(changed) > limit,
      unchanged = isTRUE(include_unchanged) && length(unchanged) > limit
    )
  )
}

wf_scope_diff <- function(source_scope, target_scope, include_versions = TRUE, limit = 200L, include_unchanged = FALSE) {
  source_payload <- wf_export_payload(source_scope, include_versions = include_versions)
  target_payload <- wf_export_payload(target_scope, include_versions = include_versions)

  objects <- wf_diff_compare(
    source_items = source_payload$objects,
    target_items = target_payload$objects,
    key_fn = wf_diff_key_object,
    ref_fn = wf_diff_ref_object,
    signature_fn = wf_diff_signature,
    limit = limit,
    include_unchanged = include_unchanged
  )

  tags <- wf_diff_compare(
    source_items = source_payload$tags,
    target_items = target_payload$tags,
    key_fn = wf_diff_key_tag,
    ref_fn = wf_diff_ref_tag,
    signature_fn = wf_diff_signature,
    limit = limit,
    include_unchanged = include_unchanged
  )

  dependencies <- wf_diff_compare(
    source_items = source_payload$dependencies,
    target_items = target_payload$dependencies,
    key_fn = wf_diff_key_dependency,
    ref_fn = wf_diff_ref_dependency,
    signature_fn = wf_diff_signature,
    limit = limit,
    include_unchanged = include_unchanged
  )

  versions <- wf_diff_compare(
    source_items = if (isTRUE(include_versions)) source_payload$versions else list(),
    target_items = if (isTRUE(include_versions)) target_payload$versions else list(),
    key_fn = wf_diff_key_version,
    ref_fn = wf_diff_ref_version,
    signature_fn = wf_diff_signature,
    limit = limit,
    include_unchanged = include_unchanged
  )

  list(
    source_scope = source_scope,
    target_scope = target_scope,
    include_versions = isTRUE(include_versions),
    include_unchanged = isTRUE(include_unchanged),
    limit = as.integer(limit),
    summary = list(
      objects = objects$counts,
      tags = tags$counts,
      dependencies = dependencies$counts,
      versions = versions$counts
    ),
    changes = list(
      objects = objects,
      tags = tags,
      dependencies = dependencies,
      versions = versions
    )
  )
}

#* Compare workflow source and target scopes (preview for promotion).
#* @tag Workflows
#* @param include_versions Include immutable versions in diff (`true`/`false`).
#* @param include_unchanged Include unchanged refs in response (`true`/`false`).
#* @param limit:int Max detailed entries per change bucket (1..2000).
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/workflows/diff
function(res,
         source_workspace = "personal", source_project = "default", source_env = "dev",
         target_workspace = "personal", target_project = "default", target_env = "stage",
         include_versions = TRUE,
         include_unchanged = FALSE,
         limit = 200) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  source_scope <- wf_scope(source_workspace, source_project, source_env)
  target_scope <- wf_scope(target_workspace, target_project, target_env)

  wf_scope_diff(
    source_scope = source_scope,
    target_scope = target_scope,
    include_versions = wf_bool(include_versions, default = TRUE),
    limit = wf_clamp_int(limit, default = 200L, min_value = 1L, max_value = 2000L),
    include_unchanged = wf_bool(include_unchanged, default = FALSE)
  )
}
