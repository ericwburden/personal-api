#* Workflow portability endpoints for export/import/snapshots.
#* Requires bearer authentication.

wf_require_portability_tables <- function(res) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  if (!isTRUE(DBI::dbExistsTable(con, "snapshots"))) {
    res$status <- 503
    return(list(error = "Missing required table 'snapshots'. Run scripts/0000-init-duckdb.R."))
  }

  NULL
}

wf_export_rows <- function(scope) {
  contexts <- DBI::dbGetQuery(
    con,
    "
    SELECT workspace, project, env, context_id AS object_id, title, content_json, updated_at, updated_by, published_version
    FROM contexts
    WHERE workspace = ? AND project = ? AND env = ?
    ",
    params = list(scope$workspace, scope$project, scope$env)
  )
  if (nrow(contexts) > 0) {
    contexts$object_type <- "context"
    contexts$trigger_type <- ""
    contexts$enabled <- TRUE
  }

  skills <- DBI::dbGetQuery(
    con,
    "
    SELECT workspace, project, env, skill_id AS object_id, title, content_json, updated_at, updated_by, published_version
    FROM skills
    WHERE workspace = ? AND project = ? AND env = ?
    ",
    params = list(scope$workspace, scope$project, scope$env)
  )
  if (nrow(skills) > 0) {
    skills$object_type <- "skill"
    skills$trigger_type <- ""
    skills$enabled <- TRUE
  }

  automations <- DBI::dbGetQuery(
    con,
    "
    SELECT workspace, project, env, automation_id AS object_id, title, content_json, updated_at, updated_by, published_version, trigger_type, enabled
    FROM automations
    WHERE workspace = ? AND project = ? AND env = ?
    ",
    params = list(scope$workspace, scope$project, scope$env)
  )
  if (nrow(automations) > 0) {
    automations$object_type <- "automation"
  }

  rows <- do.call(
    rbind,
    Filter(
      function(df) is.data.frame(df) && nrow(df) > 0,
      list(contexts, skills, automations)
    )
  )

  if (!is.data.frame(rows) || nrow(rows) == 0) {
    return(list(objects = list(), tags = list(), dependencies = list()))
  }

  objects <- lapply(
    seq_len(nrow(rows)),
    function(i) {
      row <- rows[i, , drop = FALSE]
      list(
        object_type = as.character(row$object_type[[1]]),
        object_id = as.character(row$object_id[[1]]),
        title = as.character(row$title[[1]] %||% ""),
        content = wf_json_decode(row$content_json[[1]]),
        trigger_type = as.character(row$trigger_type[[1]] %||% ""),
        enabled = isTRUE(row$enabled[[1]] %||% FALSE),
        updated_at = as.character(row$updated_at[[1]]),
        updated_by = as.character(row$updated_by[[1]] %||% ""),
        published_version = as.integer(row$published_version[[1]] %||% 0L)
      )
    }
  )

  tag_rows <- wf_list_tags(scope)
  tags <- if (nrow(tag_rows) == 0) {
    list()
  } else {
    lapply(
      seq_len(nrow(tag_rows)),
      function(i) {
        list(
          object_type = as.character(tag_rows$object_type[[i]]),
          object_id = as.character(tag_rows$object_id[[i]]),
          tag = as.character(tag_rows$tag[[i]])
        )
      }
    )
  }

  dep_rows <- wf_list_dependencies(scope)
  dependencies <- if (nrow(dep_rows) == 0) {
    list()
  } else {
    lapply(
      seq_len(nrow(dep_rows)),
      function(i) {
        list(
          source_type = as.character(dep_rows$source_type[[i]]),
          source_id = as.character(dep_rows$source_id[[i]]),
          target_type = as.character(dep_rows$target_type[[i]]),
          target_id = as.character(dep_rows$target_id[[i]]),
          required = isTRUE(dep_rows$required[[i]] %||% TRUE)
        )
      }
    )
  }

  list(
    objects = objects,
    tags = tags,
    dependencies = dependencies
  )
}

wf_export_payload <- function(scope, include_versions = FALSE) {
  rows <- wf_export_rows(scope)

  versions <- list()
  if (isTRUE(include_versions)) {
    version_rows <- DBI::dbGetQuery(
      con,
      "
      SELECT object_type, object_id, version, title, content_json, tags_json, dependencies_json, source_updated_at, created_at, created_by, change_note
      FROM versions
      WHERE workspace = ? AND project = ? AND env = ?
      ORDER BY object_type, object_id, version
      ",
      params = list(scope$workspace, scope$project, scope$env)
    )

    if (nrow(version_rows) > 0) {
      versions <- lapply(
        seq_len(nrow(version_rows)),
        function(i) {
          row <- version_rows[i, , drop = FALSE]
          list(
            object_type = as.character(row$object_type[[1]]),
            object_id = as.character(row$object_id[[1]]),
            version = as.integer(row$version[[1]]),
            title = as.character(row$title[[1]] %||% ""),
            content = wf_json_decode(row$content_json[[1]]),
            tags = wf_json_decode(row$tags_json[[1]]),
            dependencies = wf_json_decode(row$dependencies_json[[1]]),
            source_updated_at = as.character(row$source_updated_at[[1]]),
            created_at = as.character(row$created_at[[1]]),
            created_by = as.character(row$created_by[[1]] %||% ""),
            change_note = as.character(row$change_note[[1]] %||% "")
          )
        }
      )
    }
  }

  list(
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    exported_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    objects = rows$objects,
    tags = rows$tags,
    dependencies = rows$dependencies,
    versions = versions
  )
}

wf_import_payload <- function(payload, scope, strategy = "upsert") {
  strategy <- tolower(trimws(as.character(strategy %||% "upsert")))
  if (!(strategy %in% c("upsert", "replace"))) {
    strategy <- "upsert"
  }

  objects <- payload$objects %||% list()
  tags <- payload$tags %||% list()
  dependencies <- payload$dependencies %||% list()
  versions <- payload$versions %||% list()

  if (strategy == "replace") {
    DBI::dbExecute(con, "DELETE FROM dependencies WHERE workspace = ? AND project = ? AND env = ?", params = list(scope$workspace, scope$project, scope$env))
    DBI::dbExecute(con, "DELETE FROM tags WHERE workspace = ? AND project = ? AND env = ?", params = list(scope$workspace, scope$project, scope$env))
    DBI::dbExecute(con, "DELETE FROM versions WHERE workspace = ? AND project = ? AND env = ?", params = list(scope$workspace, scope$project, scope$env))
    DBI::dbExecute(con, "DELETE FROM automations WHERE workspace = ? AND project = ? AND env = ?", params = list(scope$workspace, scope$project, scope$env))
    DBI::dbExecute(con, "DELETE FROM skills WHERE workspace = ? AND project = ? AND env = ?", params = list(scope$workspace, scope$project, scope$env))
    DBI::dbExecute(con, "DELETE FROM contexts WHERE workspace = ? AND project = ? AND env = ?", params = list(scope$workspace, scope$project, scope$env))
  }

  imported_counts <- list(objects = 0L, tags = 0L, dependencies = 0L, versions = 0L)
  imported_keys <- character()

  if (length(objects) > 0) {
    for (obj in objects) {
      object_type <- wf_object_type(obj$object_type)
      object_id <- wf_id(obj$object_id)
      if (is.null(object_type) || is.null(object_id)) {
        next
      }

      title <- trimws(as.character(obj$title %||% ""))
      content_json <- wf_json_encode(obj$content %||% list())
      updated_by <- trimws(as.character(obj$updated_by %||% ""))
      published_version <- suppressWarnings(as.integer(obj$published_version %||% 0L))
      if (is.na(published_version) || published_version < 0L) {
        published_version <- 0L
      }

      if (identical(object_type, "context")) {
        updated <- DBI::dbExecute(
          con,
          "
          UPDATE contexts
          SET title = ?,
              content_json = ?,
              updated_at = CURRENT_TIMESTAMP,
              updated_by = ?,
              published_version = ?
          WHERE workspace = ?
            AND project = ?
            AND env = ?
            AND context_id = ?
          ",
          params = list(title, content_json, updated_by, published_version, scope$workspace, scope$project, scope$env, object_id)
        )

        if (as.integer(updated) == 0L) {
          DBI::dbExecute(
            con,
            "
            INSERT INTO contexts (workspace, project, env, context_id, title, content_json, updated_at, updated_by, published_version)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
            ",
            params = list(scope$workspace, scope$project, scope$env, object_id, title, content_json, updated_by, published_version)
          )
        }
      } else if (identical(object_type, "skill")) {
        updated <- DBI::dbExecute(
          con,
          "
          UPDATE skills
          SET title = ?,
              content_json = ?,
              updated_at = CURRENT_TIMESTAMP,
              updated_by = ?,
              published_version = ?
          WHERE workspace = ?
            AND project = ?
            AND env = ?
            AND skill_id = ?
          ",
          params = list(title, content_json, updated_by, published_version, scope$workspace, scope$project, scope$env, object_id)
        )

        if (as.integer(updated) == 0L) {
          DBI::dbExecute(
            con,
            "
            INSERT INTO skills (workspace, project, env, skill_id, title, content_json, updated_at, updated_by, published_version)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
            ",
            params = list(scope$workspace, scope$project, scope$env, object_id, title, content_json, updated_by, published_version)
          )
        }
      } else if (identical(object_type, "automation")) {
        trigger_type <- trimws(as.character(obj$trigger_type %||% ""))
        enabled <- wf_bool(obj$enabled, default = TRUE)
        updated <- DBI::dbExecute(
          con,
          "
          UPDATE automations
          SET title = ?,
              trigger_type = ?,
              enabled = ?,
              content_json = ?,
              updated_at = CURRENT_TIMESTAMP,
              updated_by = ?,
              published_version = ?
          WHERE workspace = ?
            AND project = ?
            AND env = ?
            AND automation_id = ?
          ",
          params = list(title, trigger_type, enabled, content_json, updated_by, published_version, scope$workspace, scope$project, scope$env, object_id)
        )

        if (as.integer(updated) == 0L) {
          DBI::dbExecute(
            con,
            "
            INSERT INTO automations (workspace, project, env, automation_id, title, trigger_type, enabled, content_json, updated_at, updated_by, published_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
            ",
            params = list(scope$workspace, scope$project, scope$env, object_id, title, trigger_type, enabled, content_json, updated_by, published_version)
          )
        }
      }

      imported_keys <- c(imported_keys, paste0(object_type, ":", object_id))
      imported_counts$objects <- imported_counts$objects + 1L
    }
  }

  if (length(imported_keys) > 0) {
    key_rows <- strsplit(unique(imported_keys), ":", fixed = TRUE)
    for (k in key_rows) {
      if (length(k) != 2) {
        next
      }
      DBI::dbExecute(
        con,
        "
        DELETE FROM tags
        WHERE workspace = ?
          AND project = ?
          AND env = ?
          AND object_type = ?
          AND object_id = ?
        ",
        params = list(scope$workspace, scope$project, scope$env, k[[1]], k[[2]])
      )
    }
  }

  if (length(tags) > 0) {
    for (tg in tags) {
      object_type <- wf_object_type(tg$object_type)
      object_id <- wf_id(tg$object_id)
      tag <- wf_id(tg$tag)
      if (is.null(object_type) || is.null(object_id) || is.null(tag)) {
        next
      }
      if (is.null(wf_find_object(object_type, object_id, scope))) {
        next
      }
      DBI::dbExecute(
        con,
        "
        INSERT INTO tags (workspace, project, env, object_type, object_id, tag, created_at)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (workspace, project, env, object_type, object_id, tag) DO NOTHING
        ",
        params = list(scope$workspace, scope$project, scope$env, object_type, object_id, tag)
      )
      imported_counts$tags <- imported_counts$tags + 1L
    }
  }

  if (length(imported_keys) > 0) {
    key_rows <- strsplit(unique(imported_keys), ":", fixed = TRUE)
    for (k in key_rows) {
      if (length(k) != 2 || !identical(k[[1]], "automation")) {
        next
      }
      DBI::dbExecute(
        con,
        "
        DELETE FROM dependencies
        WHERE workspace = ?
          AND project = ?
          AND env = ?
          AND source_type = 'automation'
          AND source_id = ?
        ",
        params = list(scope$workspace, scope$project, scope$env, k[[2]])
      )
    }
  }

  if (length(dependencies) > 0) {
    for (dep in dependencies) {
      source_type <- wf_object_type(dep$source_type)
      source_id <- wf_id(dep$source_id)
      target_type <- wf_object_type(dep$target_type)
      target_id <- wf_id(dep$target_id)
      required <- wf_bool(dep$required, default = TRUE)
      if (is.null(source_type) || is.null(source_id) || is.null(target_type) || is.null(target_id)) {
        next
      }
      if (is.null(wf_find_object(source_type, source_id, scope))) {
        next
      }
      if (is.null(wf_find_object(target_type, target_id, scope))) {
        next
      }
      DBI::dbExecute(
        con,
        "
        INSERT INTO dependencies (workspace, project, env, source_type, source_id, target_type, target_id, required, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (workspace, project, env, source_type, source_id, target_type, target_id)
        DO UPDATE SET required = EXCLUDED.required
        ",
        params = list(scope$workspace, scope$project, scope$env, source_type, source_id, target_type, target_id, required)
      )
      imported_counts$dependencies <- imported_counts$dependencies + 1L
    }
  }

  if (length(versions) > 0) {
    for (ver in versions) {
      object_type <- wf_object_type(ver$object_type)
      object_id <- wf_id(ver$object_id)
      version <- suppressWarnings(as.integer(ver$version %||% NA_integer_))
      if (is.null(object_type) || is.null(object_id) || is.na(version) || version < 1L) {
        next
      }
      DBI::dbExecute(
        con,
        "
        INSERT INTO versions (
          workspace, project, env, object_type, object_id, version, title, content_json, tags_json, dependencies_json, source_updated_at, created_at, created_by, change_note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
        ON CONFLICT (workspace, project, env, object_type, object_id, version) DO NOTHING
        ",
        params = list(
          scope$workspace,
          scope$project,
          scope$env,
          object_type,
          object_id,
          version,
          as.character(ver$title %||% ""),
          wf_json_encode(ver$content %||% list()),
          wf_json_encode(ver$tags %||% list()),
          wf_json_encode(ver$dependencies %||% list()),
          as.character(ver$source_updated_at %||% ""),
          as.character(ver$created_by %||% ""),
          as.character(ver$change_note %||% "")
        )
      )
      imported_counts$versions <- imported_counts$versions + 1L
    }
  }

  list(
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    strategy = strategy,
    imported = imported_counts
  )
}

#* Export scoped workflow objects/tags/dependencies for portability.
#* @tag Workflows
#* @param include_versions Include immutable versions in export payload (`true`/`false`).
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/export
function(res, workspace = "personal", project = "default", env = "dev", include_versions = FALSE) {
  missing <- wf_require_portability_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  scope <- wf_scope(workspace, project, env)
  wf_export_payload(scope = scope, include_versions = wf_bool(include_versions, default = FALSE))
}

#* Import workflow payload into scope.
#* Body may be either payload root or `{ "payload": { ... } }`.
#* @tag Workflows
#* @param strategy Import strategy: `upsert` or `replace`.
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/import
function(req, res, workspace = "personal", project = "default", env = "dev", strategy = "upsert") {
  missing <- wf_require_portability_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  parsed <- wf_json_parse(req, res)
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  payload <- parsed$payload
  if (is.null(payload) || !is.list(payload)) {
    payload <- parsed
  }

  scope <- wf_scope(
    workspace = workspace %||% payload$workspace %||% "personal",
    project = project %||% payload$project %||% "default",
    env = env %||% payload$env %||% "dev"
  )

  out <- wf_import_payload(payload = payload, scope = scope, strategy = strategy)
  out$status <- "imported"
  out
}

#* Create a persisted snapshot for scope.
#* Body may include `note` and `created_by`.
#* @tag Workflows
#* @param include_versions Include immutable versions in snapshot payload (`true`/`false`).
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/snapshots
function(req, res, workspace = "personal", project = "default", env = "dev", include_versions = TRUE) {
  missing <- wf_require_portability_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  parsed <- wf_json_parse(req, res)
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  scope <- wf_scope(workspace, project, env)
  payload <- wf_export_payload(scope = scope, include_versions = wf_bool(include_versions, default = TRUE))

  snapshot_id <- uuid::UUIDgenerate()
  note <- trimws(as.character(parsed$note %||% ""))
  created_by <- trimws(as.character(parsed$created_by %||% ""))

  DBI::dbExecute(
    con,
    "
    INSERT INTO snapshots (snapshot_id, workspace, project, env, note, created_by, created_at, payload_json)
    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
    ",
    params = list(
      snapshot_id,
      scope$workspace,
      scope$project,
      scope$env,
      note,
      created_by,
      wf_json_encode(payload)
    )
  )

  list(
    status = "created",
    snapshot_id = snapshot_id,
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env
  )
}

#* List snapshots for scope.
#* @tag Workflows
#* @param limit:int Maximum rows to return (1..1000).
#* @param offset:int Rows to skip.
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/snapshots
function(res, workspace = "personal", project = "default", env = "dev", limit = 50, offset = 0) {
  missing <- wf_require_portability_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  limit <- wf_clamp_int(limit, default = 50L, min_value = 1L, max_value = 1000L)
  offset <- wf_clamp_int(offset, default = 0L, min_value = 0L, max_value = 100000L)

  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT snapshot_id, workspace, project, env, note, created_by, created_at
      FROM snapshots
      WHERE workspace = ?
        AND project = ?
        AND env = ?
      ORDER BY created_at DESC
      LIMIT %d
      OFFSET %d
      ",
      limit,
      offset
    ),
    params = list(workspace, project, env)
  )
}

#* Get one snapshot payload by id.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/snapshots/<snapshot_id>
function(res, snapshot_id) {
  missing <- wf_require_portability_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  id <- wf_id(snapshot_id)
  if (is.null(id)) {
    res$status <- 400
    return(list(error = "Snapshot id is required"))
  }

  out <- DBI::dbGetQuery(
    con,
    "
    SELECT snapshot_id, workspace, project, env, note, created_by, created_at, payload_json
    FROM snapshots
    WHERE snapshot_id = ?
    LIMIT 1
    ",
    params = list(id)
  )

  if (nrow(out) == 0) {
    res$status <- 404
    return(list(error = "Snapshot not found"))
  }

  list(
    snapshot_id = as.character(out$snapshot_id[[1]]),
    workspace = as.character(out$workspace[[1]]),
    project = as.character(out$project[[1]]),
    env = as.character(out$env[[1]]),
    note = as.character(out$note[[1]] %||% ""),
    created_by = as.character(out$created_by[[1]] %||% ""),
    created_at = as.character(out$created_at[[1]]),
    payload = wf_json_decode(as.character(out$payload_json[[1]]))
  )
}

#* Restore workflow objects from a snapshot id.
#* Body may include `strategy` (`upsert` or `replace`).
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/snapshots/<snapshot_id>/restore
function(req, res, snapshot_id, workspace = "personal", project = "default", env = "dev", strategy = "replace") {
  missing <- wf_require_portability_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  id <- wf_id(snapshot_id)
  if (is.null(id)) {
    res$status <- 400
    return(list(error = "Snapshot id is required"))
  }

  row <- DBI::dbGetQuery(
    con,
    "
    SELECT payload_json
    FROM snapshots
    WHERE snapshot_id = ?
    LIMIT 1
    ",
    params = list(id)
  )

  if (nrow(row) == 0) {
    res$status <- 404
    return(list(error = "Snapshot not found"))
  }

  parsed <- wf_json_parse(req, res)
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  effective_strategy <- trimws(as.character(parsed$strategy %||% strategy %||% "replace"))
  payload <- wf_json_decode(as.character(row$payload_json[[1]]))
  target_scope <- wf_scope(
    workspace = workspace %||% payload$workspace %||% "personal",
    project = project %||% payload$project %||% "default",
    env = env %||% payload$env %||% "dev"
  )

  out <- wf_import_payload(payload = payload, scope = target_scope, strategy = effective_strategy)
  out$status <- "restored"
  out$snapshot_id <- id
  out
}
