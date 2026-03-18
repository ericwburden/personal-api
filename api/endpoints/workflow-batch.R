#* Workflow batch endpoints for upsert/publish operations.
#* Requires bearer authentication.

wf_batch_items <- function(parsed, field_name = "objects") {
  items <- parsed[[field_name]]
  if (is.null(items) || !is.list(items) || length(items) == 0L) {
    return(NULL)
  }
  items
}

wf_batch_error <- function(index, object_type = NULL, object_id = NULL, error = "") {
  list(
    index = as.integer(index),
    object_type = as.character(object_type %||% ""),
    object_id = as.character(object_id %||% ""),
    error = as.character(error %||% "")
  )
}

wf_batch_status <- function(res, success_count, error_count) {
  if (error_count == 0L) {
    return(invisible(NULL))
  }

  if (success_count > 0L) {
    res$status <- 207
  } else {
    res$status <- 400
  }

  invisible(NULL)
}

#* Upsert multiple workflow objects in one request.
#* Body: `{ "objects": [{ "object_type": "context|skill|automation", "object_id": "...", "title": "...", "content": {...}, "updated_by": "..." }], "continue_on_error": false }`.
#* For automations, include optional `trigger_type` and `enabled`.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/objects/batch-upsert
function(req, res, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  parsed <- wf_json_parse(req, res)
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  objects <- wf_batch_items(parsed, field_name = "objects")
  if (is.null(objects)) {
    res$status <- 400
    return(list(error = "Field 'objects' must be a non-empty list"))
  }

  scope <- wf_scope(workspace, project, env)
  continue_on_error <- wf_bool(parsed$continue_on_error, default = FALSE)

  saved <- list()
  errors <- list()

  for (i in seq_along(objects)) {
    obj <- objects[[i]]

    if (!is.list(obj)) {
      errors[[length(errors) + 1L]] <- wf_batch_error(i, error = "Each object entry must be an object")
      if (!continue_on_error) {
        break
      }
      next
    }

    object_type <- wf_object_type(obj$object_type)
    object_id <- wf_id(obj$object_id)

    if (is.null(object_type) || is.null(object_id)) {
      errors[[length(errors) + 1L]] <- wf_batch_error(i, obj$object_type, obj$object_id, "Fields object_type/object_id are required")
      if (!continue_on_error) {
        break
      }
      next
    }

    upsert_payload <- list(
      title = obj$title %||% "",
      content = obj$content %||% obj$body %||% list(),
      updated_by = obj$updated_by %||% "",
      trigger_type = obj$trigger_type %||% "",
      enabled = obj$enabled %||% TRUE
    )

    item_res <- list(status = 200L)
    result <- wf_upsert_object_from_parsed(item_res, object_type, object_id, scope, upsert_payload)

    if (!is.list(result) || !is.null(result$error)) {
      errors[[length(errors) + 1L]] <- wf_batch_error(i, object_type, object_id, result$error %||% "Failed to upsert object")
      if (!continue_on_error) {
        break
      }
      next
    }

    saved[[length(saved) + 1L]] <- list(
      index = as.integer(i),
      object_type = object_type,
      object_id = object_id,
      status = as.character(result$status %||% "saved"),
      object = result$object
    )
  }

  wf_batch_status(res, length(saved), length(errors))

  list(
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    saved_count = as.integer(length(saved)),
    error_count = as.integer(length(errors)),
    saved = saved,
    errors = errors
  )
}

#* Publish multiple workflow objects in one request.
#* Body: `{ "refs": [{ "object_type": "context|skill|automation", "object_id": "..." }], "updated_by": "...", "change_note": "...", "continue_on_error": false }`.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/objects/batch-publish
function(req, res, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  parsed <- wf_json_parse(req, res)
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  refs <- wf_batch_items(parsed, field_name = "refs")
  if (is.null(refs)) {
    res$status <- 400
    return(list(error = "Field 'refs' must be a non-empty list"))
  }

  scope <- wf_scope(workspace, project, env)
  continue_on_error <- wf_bool(parsed$continue_on_error, default = FALSE)

  publish_payload <- list(
    updated_by = parsed$updated_by %||% parsed$created_by %||% "",
    change_note = parsed$change_note %||% ""
  )

  published <- list()
  errors <- list()

  for (i in seq_along(refs)) {
    ref <- refs[[i]]

    if (!is.list(ref)) {
      errors[[length(errors) + 1L]] <- wf_batch_error(i, error = "Each ref entry must be an object")
      if (!continue_on_error) {
        break
      }
      next
    }

    object_type <- wf_object_type(ref$object_type)
    object_id <- wf_id(ref$object_id)

    if (is.null(object_type) || is.null(object_id)) {
      errors[[length(errors) + 1L]] <- wf_batch_error(i, ref$object_type, ref$object_id, "Fields object_type/object_id are required")
      if (!continue_on_error) {
        break
      }
      next
    }

    item_res <- list(status = 200L)
    result <- wf_publish_object_from_parsed(item_res, object_type, object_id, scope, publish_payload)

    if (!is.list(result) || !is.null(result$error)) {
      errors[[length(errors) + 1L]] <- wf_batch_error(i, object_type, object_id, result$error %||% "Failed to publish object")
      if (!continue_on_error) {
        break
      }
      next
    }

    published[[length(published) + 1L]] <- list(
      index = as.integer(i),
      object_type = object_type,
      object_id = object_id,
      status = as.character(result$status %||% "published"),
      version = as.integer(result$version %||% 0L)
    )
  }

  wf_batch_status(res, length(published), length(errors))

  list(
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    published_count = as.integer(length(published)),
    error_count = as.integer(length(errors)),
    published = published,
    errors = errors
  )
}

#* Delete multiple workflow objects in one request.
#* Body: `{ "refs": [{ "object_type": "context|skill|automation", "object_id": "..." }], "delete_versions": true, "continue_on_error": false }`.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/objects/batch-delete
function(req, res, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  parsed <- wf_json_parse(req, res)
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  refs <- wf_batch_items(parsed, field_name = "refs")
  if (is.null(refs)) {
    res$status <- 400
    return(list(error = "Field 'refs' must be a non-empty list"))
  }

  scope <- wf_scope(workspace, project, env)
  continue_on_error <- wf_bool(parsed$continue_on_error, default = FALSE)
  delete_versions <- wf_bool(parsed$delete_versions, default = TRUE)

  deleted <- list()
  errors <- list()

  for (i in seq_along(refs)) {
    ref <- refs[[i]]

    if (!is.list(ref)) {
      errors[[length(errors) + 1L]] <- wf_batch_error(i, error = "Each ref entry must be an object")
      if (!continue_on_error) {
        break
      }
      next
    }

    object_type <- wf_object_type(ref$object_type)
    object_id <- wf_id(ref$object_id)

    if (is.null(object_type) || is.null(object_id)) {
      errors[[length(errors) + 1L]] <- wf_batch_error(i, ref$object_type, ref$object_id, "Fields object_type/object_id are required")
      if (!continue_on_error) {
        break
      }
      next
    }

    item_res <- list(status = 200L)
    result <- wf_delete_object(
      res = item_res,
      object_type = object_type,
      object_id = object_id,
      scope = scope,
      delete_versions = delete_versions
    )

    if (!is.list(result) || !is.null(result$error)) {
      errors[[length(errors) + 1L]] <- wf_batch_error(i, object_type, object_id, result$error %||% "Failed to delete object")
      if (!continue_on_error) {
        break
      }
      next
    }

    deleted[[length(deleted) + 1L]] <- list(
      index = as.integer(i),
      object_type = object_type,
      object_id = object_id,
      status = as.character(result$status %||% "deleted"),
      deleted = result$deleted
    )
  }

  wf_batch_status(res, length(deleted), length(errors))

  list(
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    delete_versions = delete_versions,
    deleted_count = as.integer(length(deleted)),
    error_count = as.integer(length(errors)),
    deleted = deleted,
    errors = errors
  )
}
