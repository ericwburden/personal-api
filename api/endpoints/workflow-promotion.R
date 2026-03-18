#* Workflow promotion endpoint for cross-scope copy/sync.
#* Requires bearer authentication.

wf_scope_counts <- function(scope) {
  list(
    contexts = wf_scope_table_count("contexts", scope),
    skills = wf_scope_table_count("skills", scope),
    automations = wf_scope_table_count("automations", scope),
    tags = wf_scope_table_count("tags", scope),
    dependencies = wf_scope_table_count("dependencies", scope),
    versions = wf_scope_table_count("versions", scope)
  )
}

wf_payload_counts <- function(payload) {
  list(
    objects = as.integer(length(payload$objects %||% list())),
    tags = as.integer(length(payload$tags %||% list())),
    dependencies = as.integer(length(payload$dependencies %||% list())),
    versions = as.integer(length(payload$versions %||% list()))
  )
}

wf_payload_object_sample <- function(payload, limit = 10L) {
  objects <- payload$objects %||% list()
  if (length(objects) == 0L) {
    return(list())
  }

  out <- list()
  max_n <- min(as.integer(limit), length(objects))

  for (i in seq_len(max_n)) {
    obj <- objects[[i]]
    out[[length(out) + 1L]] <- list(
      object_type = as.character(obj$object_type %||% ""),
      object_id = as.character(obj$object_id %||% "")
    )
  }

  out
}

wf_promotion_strategy <- function(res, strategy) {
  value <- tolower(trimws(as.character(strategy %||% "upsert")))
  if (!(value %in% c("upsert", "replace"))) {
    res$status <- 400
    return(list(error = "Invalid strategy. Use 'upsert' or 'replace'."))
  }
  value
}

#* Promote workflow payload from a source scope to a target scope.
#* Request body/query fields:
#* `source_workspace`, `source_project`, `source_env`, `target_workspace`, `target_project`, `target_env`,
#* `strategy` (`upsert|replace`), `include_versions` (`true|false`), `dry_run` (`true|false`).
#* `dry_run` defaults to `true`.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/promote
function(req, res,
         source_workspace = "personal", source_project = "default", source_env = "dev",
         target_workspace = "personal", target_project = "default", target_env = "stage",
         strategy = "upsert", include_versions = TRUE, dry_run = TRUE) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  parsed <- wf_json_parse_optional(req, res, default = list())
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  source_scope <- wf_scope(
    parsed$source_workspace %||% source_workspace,
    parsed$source_project %||% source_project,
    parsed$source_env %||% source_env
  )

  target_scope <- wf_scope(
    parsed$target_workspace %||% target_workspace,
    parsed$target_project %||% target_project,
    parsed$target_env %||% target_env
  )

  strategy_value <- wf_promotion_strategy(res, parsed$strategy %||% strategy)
  if (is.list(strategy_value) && !is.null(strategy_value$error)) {
    return(strategy_value)
  }

  include_versions_value <- wf_bool(parsed$include_versions %||% include_versions, default = TRUE)
  dry_run_value <- wf_bool(parsed$dry_run %||% dry_run, default = TRUE)

  payload <- wf_export_payload(scope = source_scope, include_versions = include_versions_value)
  payload_counts <- wf_payload_counts(payload)
  target_before <- wf_scope_counts(target_scope)

  if (isTRUE(dry_run_value)) {
    return(
      list(
        status = "preview",
        dry_run = TRUE,
        strategy = strategy_value,
        include_versions = include_versions_value,
        source_scope = source_scope,
        target_scope = target_scope,
        payload_counts = payload_counts,
        target_counts_before = target_before,
        sample_objects = wf_payload_object_sample(payload, limit = 10L)
      )
    )
  }

  imported <- wf_import_payload(payload = payload, scope = target_scope, strategy = strategy_value)
  target_after <- wf_scope_counts(target_scope)

  list(
    status = "promoted",
    dry_run = FALSE,
    strategy = strategy_value,
    include_versions = include_versions_value,
    source_scope = source_scope,
    target_scope = target_scope,
    payload_counts = payload_counts,
    imported = imported$imported,
    target_counts_before = target_before,
    target_counts_after = target_after
  )
}
