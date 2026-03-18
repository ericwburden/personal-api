#* Workflow storage endpoints for contexts, skills, and automations.
#* Requires bearer authentication.

wf_object_specs <- list(
  context = list(table = "contexts", id_col = "context_id"),
  skill = list(table = "skills", id_col = "skill_id"),
  automation = list(table = "automations", id_col = "automation_id")
)

wf_required_tables <- c("contexts", "skills", "automations", "versions", "tags", "dependencies")

wf_scope <- function(workspace = "personal", project = "default", env = "dev") {
  clean <- function(x, default) {
    value <- trimws(as.character(x %||% default))
    if (!nzchar(value)) {
      return(default)
    }
    value
  }

  list(
    workspace = clean(workspace, "personal"),
    project = clean(project, "default"),
    env = clean(env, "dev")
  )
}

wf_clamp_int <- function(x, default, min_value, max_value) {
  out <- suppressWarnings(as.integer(x))
  if (length(out) == 0L || is.na(out[[1]])) {
    out <- default
  } else {
    out <- out[[1]]
  }
  out <- max(out, min_value)
  min(out, max_value)
}

wf_sort_order <- function(order, default = "desc") {
  value <- tolower(trimws(as.character(order %||% default)))
  if (!(value %in% c("asc", "desc"))) {
    return(default)
  }
  value
}

wf_sort_field <- function(sort_by, allowed, default) {
  value <- trimws(as.character(sort_by %||% default))
  if (!nzchar(value) || !(value %in% allowed)) {
    return(default)
  }
  value
}

wf_object_type <- function(object_type) {
  out <- tolower(trimws(as.character(object_type %||% "")))
  if (!(out %in% names(wf_object_specs))) {
    return(NULL)
  }
  out
}

wf_id <- function(id) {
  value <- trimws(as.character(id %||% ""))
  if (!nzchar(value)) {
    return(NULL)
  }
  value
}

wf_json_parse <- function(req, res) {
  parsed <- tryCatch(
    jsonlite::fromJSON(req$postBody %||% "", simplifyVector = FALSE),
    error = function(e) NULL
  )

  if (is.null(parsed) || !is.list(parsed)) {
    res$status <- 400
    return(list(error = "Invalid JSON body"))
  }

  parsed
}

wf_json_parse_optional <- function(req, res, default = list()) {
  body <- as.character(req$postBody %||% "")
  if (!nzchar(trimws(body))) {
    return(default)
  }

  parsed <- tryCatch(
    jsonlite::fromJSON(body, simplifyVector = FALSE),
    error = function(e) NULL
  )

  if (is.null(parsed) || !is.list(parsed)) {
    res$status <- 400
    return(list(error = "Invalid JSON body"))
  }

  parsed
}

wf_json_encode <- function(value) {
  jsonlite::toJSON(value, auto_unbox = TRUE, null = "null")
}

wf_json_decode <- function(value) {
  if (is.null(value) || is.na(value) || !nzchar(value)) {
    return(NULL)
  }

  tryCatch(
    jsonlite::fromJSON(value, simplifyVector = FALSE),
    error = function(e) value
  )
}

wf_missing_table <- function(res, table_name) {
  res$status <- 503
  list(error = paste0("Missing required table '", table_name, "'. Run scripts/0000-init-duckdb.R."))
}

wf_bool <- function(x, default = FALSE) {
  if (is.logical(x) && length(x) == 1 && !is.na(x)) {
    return(isTRUE(x))
  }

  value <- tolower(trimws(as.character(x %||% "")))
  if (!nzchar(value)) {
    return(default)
  }
  value %in% c("1", "true", "yes", "y", "on")
}

wf_ensure_tables <- function(res) {
  for (tbl in wf_required_tables) {
    if (!isTRUE(DBI::dbExistsTable(con, tbl))) {
      return(wf_missing_table(res, tbl))
    }
  }

  NULL
}

wf_find_object <- function(object_type, object_id, scope) {
  spec <- wf_object_specs[[object_type]]
  if (is.null(spec)) {
    return(NULL)
  }

  sql <- sprintf(
    "
    SELECT *
    FROM %s
    WHERE workspace = ?
      AND project = ?
      AND env = ?
      AND %s = ?
    LIMIT 1
    ",
    spec$table,
    spec$id_col
  )

  out <- DBI::dbGetQuery(
    con,
    sql,
    params = list(scope$workspace, scope$project, scope$env, object_id)
  )

  if (nrow(out) == 0) {
    return(NULL)
  }

  out[1, , drop = FALSE]
}

wf_get_tags <- function(object_type, object_id, scope) {
  out <- DBI::dbGetQuery(
    con,
    "
    SELECT tag
    FROM tags
    WHERE workspace = ?
      AND project = ?
      AND env = ?
      AND object_type = ?
      AND object_id = ?
    ORDER BY tag
    ",
    params = list(scope$workspace, scope$project, scope$env, object_type, object_id)
  )

  as.character(out$tag %||% character())
}

wf_get_dependencies <- function(source_type, source_id, scope) {
  DBI::dbGetQuery(
    con,
    "
    SELECT source_type, source_id, target_type, target_id, required, created_at
    FROM dependencies
    WHERE workspace = ?
      AND project = ?
      AND env = ?
      AND source_type = ?
      AND source_id = ?
    ORDER BY target_type, target_id
    ",
    params = list(scope$workspace, scope$project, scope$env, source_type, source_id)
  )
}

wf_next_version <- function(object_type, object_id, scope) {
  out <- DBI::dbGetQuery(
    con,
    "
    SELECT COALESCE(MAX(version), 0) + 1 AS next_version
    FROM versions
    WHERE workspace = ?
      AND project = ?
      AND env = ?
      AND object_type = ?
      AND object_id = ?
    ",
    params = list(scope$workspace, scope$project, scope$env, object_type, object_id)
  )

  as.integer(out$next_version[[1]])
}

wf_object_to_response <- function(object_type, row, scope, version = NULL) {
  spec <- wf_object_specs[[object_type]]
  id_col <- spec$id_col
  object_id <- as.character(row[[id_col]][[1]])
  tags <- wf_get_tags(object_type, object_id, scope)
  dependencies <- NULL

  if (identical(object_type, "automation")) {
    dependencies <- wf_get_dependencies("automation", object_id, scope)
  }

  out <- list(
    object_type = object_type,
    object_id = object_id,
    workspace = as.character(row$workspace[[1]]),
    project = as.character(row$project[[1]]),
    env = as.character(row$env[[1]]),
    title = as.character(row$title[[1]] %||% ""),
    content = wf_json_decode(row$content_json[[1]]),
    tags = tags,
    dependencies = dependencies,
    updated_at = as.character(row$updated_at[[1]]),
    updated_by = as.character(row$updated_by[[1]] %||% ""),
    published_version = as.integer(row$published_version[[1]] %||% 0L),
    version = version
  )

  if (identical(object_type, "automation")) {
    out$trigger_type <- as.character(row$trigger_type[[1]] %||% "")
    out$enabled <- isTRUE(row$enabled[[1]] %||% FALSE)
  }

  out
}

wf_version_to_response <- function(row) {
  list(
    object_type = as.character(row$object_type[[1]]),
    object_id = as.character(row$object_id[[1]]),
    workspace = as.character(row$workspace[[1]]),
    project = as.character(row$project[[1]]),
    env = as.character(row$env[[1]]),
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

wf_list_objects <- function(object_type, scope, tag = NULL, q = NULL, sort_by = "updated_at", order = "desc", limit = 100, offset = 0) {
  spec <- wf_object_specs[[object_type]]
  sort_by <- wf_sort_field(sort_by, allowed = c(spec$id_col, "title", "updated_at", "published_version"), default = "updated_at")
  order <- wf_sort_order(order, default = "desc")
  limit <- wf_clamp_int(limit, default = 100L, min_value = 1L, max_value = 1000L)
  offset <- wf_clamp_int(offset, default = 0L, min_value = 0L, max_value = 100000L)

  q <- trimws(as.character(q %||% ""))
  tag <- trimws(as.character(tag %||% ""))
  has_q <- nzchar(q)
  has_tag <- nzchar(tag)

  where <- c("workspace = ?", "project = ?", "env = ?")
  params <- list(scope$workspace, scope$project, scope$env)

  if (has_q) {
    where <- c(where, sprintf("(LOWER(%s) LIKE LOWER(?) OR LOWER(COALESCE(title, '')) LIKE LOWER(?))", spec$id_col))
    params <- c(params, list(paste0("%", q, "%"), paste0("%", q, "%")))
  }

  if (has_tag) {
    where <- c(where, sprintf("EXISTS (SELECT 1 FROM tags t WHERE t.workspace = %s.workspace AND t.project = %s.project AND t.env = %s.env AND t.object_type = ? AND t.object_id = %s.%s AND t.tag = ?)", spec$table, spec$table, spec$table, spec$table, spec$id_col))
    params <- c(params, list(object_type, tag))
  }

  where_sql <- paste(where, collapse = " AND ")

  total <- DBI::dbGetQuery(
    con,
    sprintf("SELECT COUNT(*) AS n FROM %s WHERE %s", spec$table, where_sql),
    params = params
  )

  rows <- DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT *
      FROM %s
      WHERE %s
      ORDER BY %s %s
      LIMIT %d
      OFFSET %d
      ",
      spec$table,
      where_sql,
      sort_by,
      order,
      limit,
      offset
    ),
    params = params
  )

  items <- lapply(
    seq_len(nrow(rows)),
    function(i) wf_object_to_response(object_type, rows[i, , drop = FALSE], scope = scope)
  )

  list(
    items = items,
    total = as.integer(total$n[[1]]),
    limit = limit,
    offset = offset
  )
}

wf_upsert_object_from_parsed <- function(res, object_type, object_id, scope, parsed) {
  id <- wf_id(object_id)
  if (is.null(id)) {
    res$status <- 400
    return(list(error = "Object id is required"))
  }

  title <- trimws(as.character(parsed$title %||% ""))
  content <- parsed$content %||% parsed$body %||% list()
  updated_by <- trimws(as.character(parsed$updated_by %||% ""))

  spec <- wf_object_specs[[object_type]]
  existing <- wf_find_object(object_type, id, scope)
  content_json <- wf_json_encode(content)

  if (is.null(existing)) {
    DBI::dbExecute(
      con,
      sprintf(
        "
        INSERT INTO %s (workspace, project, env, %s, title, content_json, updated_at, updated_by, published_version%s)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, 0%s)
        ",
        spec$table,
        spec$id_col,
        if (identical(object_type, "automation")) ", trigger_type, enabled" else "",
        if (identical(object_type, "automation")) ", ?, ?" else ""
      ),
      params = c(
        list(scope$workspace, scope$project, scope$env, id, title, content_json, updated_by),
        if (identical(object_type, "automation")) {
          list(trimws(as.character(parsed$trigger_type %||% "")), isTRUE(parsed$enabled %||% TRUE))
        } else {
          list()
        }
      )
    )
  } else {
    DBI::dbExecute(
      con,
      sprintf(
        "
        UPDATE %s
        SET title = ?,
            content_json = ?,
            updated_at = CURRENT_TIMESTAMP,
            updated_by = ?
            %s
        WHERE workspace = ?
          AND project = ?
          AND env = ?
          AND %s = ?
        ",
        spec$table,
        if (identical(object_type, "automation")) ", trigger_type = ?, enabled = ?" else "",
        spec$id_col
      ),
      params = c(
        list(title, content_json, updated_by),
        if (identical(object_type, "automation")) {
          list(trimws(as.character(parsed$trigger_type %||% "")), isTRUE(parsed$enabled %||% TRUE))
        } else {
          list()
        },
        list(scope$workspace, scope$project, scope$env, id)
      )
    )
  }

  row <- wf_find_object(object_type, id, scope)
  list(status = "saved", object = wf_object_to_response(object_type, row, scope = scope))
}

wf_upsert_object <- function(res, req, object_type, object_id, scope) {
  parsed <- wf_json_parse(req, res)
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  wf_upsert_object_from_parsed(
    res = res,
    object_type = object_type,
    object_id = object_id,
    scope = scope,
    parsed = parsed
  )
}

wf_get_object <- function(res, object_type, object_id, scope, version = NULL) {
  id <- wf_id(object_id)
  if (is.null(id)) {
    res$status <- 400
    return(list(error = "Object id is required"))
  }

  version_txt <- trimws(as.character(version %||% ""))
  if (!nzchar(version_txt)) {
    row <- wf_find_object(object_type, id, scope)
    if (is.null(row)) {
      res$status <- 404
      return(list(error = "Object not found"))
    }
    return(wf_object_to_response(object_type, row, scope = scope))
  }

  if (identical(tolower(version_txt), "latest")) {
    current <- wf_find_object(object_type, id, scope)
    if (is.null(current)) {
      res$status <- 404
      return(list(error = "Object not found"))
    }

    latest_version <- as.integer(current$published_version[[1]] %||% 0L)
    if (latest_version <= 0L) {
      return(wf_object_to_response(object_type, current, scope = scope))
    }

    version_txt <- as.character(latest_version)
  }

  version_no <- suppressWarnings(as.integer(version_txt))
  if (is.na(version_no) || version_no < 1L) {
    res$status <- 400
    return(list(error = "Version must be a positive integer or 'latest'"))
  }

  row <- DBI::dbGetQuery(
    con,
    "
    SELECT *
    FROM versions
    WHERE workspace = ?
      AND project = ?
      AND env = ?
      AND object_type = ?
      AND object_id = ?
      AND version = ?
    LIMIT 1
    ",
    params = list(scope$workspace, scope$project, scope$env, object_type, id, version_no)
  )

  if (nrow(row) == 0) {
    res$status <- 404
    return(list(error = "Version not found"))
  }

  wf_version_to_response(row[1, , drop = FALSE])
}

wf_publish_object_from_parsed <- function(res, object_type, object_id, scope, parsed) {
  id <- wf_id(object_id)
  if (is.null(id)) {
    res$status <- 400
    return(list(error = "Object id is required"))
  }

  current <- wf_find_object(object_type, id, scope)
  if (is.null(current)) {
    res$status <- 404
    return(list(error = "Object not found"))
  }

  created_by <- trimws(as.character(parsed$updated_by %||% parsed$created_by %||% ""))
  change_note <- trimws(as.character(parsed$change_note %||% ""))

  version_no <- wf_next_version(object_type, id, scope)
  tags <- wf_get_tags(object_type, id, scope)
  dependencies <- if (identical(object_type, "automation")) {
    wf_get_dependencies("automation", id, scope)
  } else {
    NULL
  }

  spec <- wf_object_specs[[object_type]]

  DBI::dbExecute(
    con,
    "
    INSERT INTO versions (
      workspace, project, env, object_type, object_id, version, title,
      content_json, tags_json, dependencies_json, source_updated_at,
      created_at, created_by, change_note
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
    ",
    params = list(
      scope$workspace,
      scope$project,
      scope$env,
      object_type,
      id,
      version_no,
      as.character(current$title[[1]] %||% ""),
      as.character(current$content_json[[1]]),
      wf_json_encode(tags),
      wf_json_encode(dependencies),
      as.character(current$updated_at[[1]]),
      created_by,
      change_note
    )
  )

  DBI::dbExecute(
    con,
    sprintf(
      "
      UPDATE %s
      SET published_version = ?,
          updated_at = CURRENT_TIMESTAMP
      WHERE workspace = ?
        AND project = ?
        AND env = ?
        AND %s = ?
      ",
      spec$table,
      spec$id_col
    ),
    params = list(version_no, scope$workspace, scope$project, scope$env, id)
  )

  list(status = "published", version = version_no)
}

wf_publish_object <- function(res, req, object_type, object_id, scope) {
  parsed <- wf_json_parse(req, res)
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  wf_publish_object_from_parsed(
    res = res,
    object_type = object_type,
    object_id = object_id,
    scope = scope,
    parsed = parsed
  )
}

wf_delete_object <- function(res, object_type, object_id, scope, delete_versions = TRUE) {
  id <- wf_id(object_id)
  if (is.null(id)) {
    res$status <- 400
    return(list(error = "Object id is required"))
  }

  current <- wf_find_object(object_type, id, scope)
  if (is.null(current)) {
    res$status <- 404
    return(list(error = "Object not found"))
  }

  spec <- wf_object_specs[[object_type]]
  tags_deleted <- 0L
  dependencies_deleted <- 0L
  versions_deleted <- 0L
  object_deleted <- 0L

  DBI::dbWithTransaction(
    con,
    {
      tags_deleted <<- as.integer(
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
          params = list(scope$workspace, scope$project, scope$env, object_type, id)
        )
      )

      dependencies_deleted <<- as.integer(
        DBI::dbExecute(
          con,
          "
          DELETE FROM dependencies
          WHERE workspace = ?
            AND project = ?
            AND env = ?
            AND (
              (source_type = ? AND source_id = ?)
              OR
              (target_type = ? AND target_id = ?)
            )
          ",
          params = list(scope$workspace, scope$project, scope$env, object_type, id, object_type, id)
        )
      )

      if (isTRUE(delete_versions)) {
        versions_deleted <<- as.integer(
          DBI::dbExecute(
            con,
            "
            DELETE FROM versions
            WHERE workspace = ?
              AND project = ?
              AND env = ?
              AND object_type = ?
              AND object_id = ?
            ",
            params = list(scope$workspace, scope$project, scope$env, object_type, id)
          )
        )
      }

      object_deleted <<- as.integer(
        DBI::dbExecute(
          con,
          sprintf(
            "
            DELETE FROM %s
            WHERE workspace = ?
              AND project = ?
              AND env = ?
              AND %s = ?
            ",
            spec$table,
            spec$id_col
          ),
          params = list(scope$workspace, scope$project, scope$env, id)
        )
      )
    }
  )

  list(
    status = "deleted",
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    object_type = object_type,
    object_id = id,
    delete_versions = isTRUE(delete_versions),
    deleted = list(
      object = object_deleted,
      tags = tags_deleted,
      dependencies = dependencies_deleted,
      versions = versions_deleted
    )
  )
}

wf_list_versions <- function(object_type, object_id, scope) {
  id <- wf_id(object_id)
  if (is.null(id)) {
    return(data.frame())
  }

  DBI::dbGetQuery(
    con,
    "
    SELECT
      object_type,
      object_id,
      version,
      title,
      source_updated_at,
      created_at,
      created_by,
      change_note
    FROM versions
    WHERE workspace = ?
      AND project = ?
      AND env = ?
      AND object_type = ?
      AND object_id = ?
    ORDER BY version DESC
    ",
    params = list(scope$workspace, scope$project, scope$env, object_type, id)
  )
}

wf_rollback <- function(res, req, object_type, object_id, scope) {
  id <- wf_id(object_id)
  if (is.null(id)) {
    res$status <- 400
    return(list(error = "Object id is required"))
  }

  parsed <- wf_json_parse(req, res)
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  version_no <- suppressWarnings(as.integer(parsed$version %||% NA_integer_))
  if (is.na(version_no) || version_no < 1L) {
    res$status <- 400
    return(list(error = "Field 'version' must be a positive integer"))
  }

  version_row <- DBI::dbGetQuery(
    con,
    "
    SELECT *
    FROM versions
    WHERE workspace = ?
      AND project = ?
      AND env = ?
      AND object_type = ?
      AND object_id = ?
      AND version = ?
    LIMIT 1
    ",
    params = list(scope$workspace, scope$project, scope$env, object_type, id, version_no)
  )

  if (nrow(version_row) == 0) {
    res$status <- 404
    return(list(error = "Version not found"))
  }

  spec <- wf_object_specs[[object_type]]
  updated_by <- trimws(as.character(parsed$updated_by %||% ""))

  DBI::dbExecute(
    con,
    sprintf(
      "
      UPDATE %s
      SET title = ?,
          content_json = ?,
          updated_by = ?,
          updated_at = CURRENT_TIMESTAMP,
          published_version = ?
      WHERE workspace = ?
        AND project = ?
        AND env = ?
        AND %s = ?
      ",
      spec$table,
      spec$id_col
    ),
    params = list(
      as.character(version_row$title[[1]] %||% ""),
      as.character(version_row$content_json[[1]]),
      updated_by,
      version_no,
      scope$workspace,
      scope$project,
      scope$env,
      id
    )
  )

  tags <- wf_json_decode(version_row$tags_json[[1]])
  if (is.null(tags)) {
    tags <- character()
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
    params = list(scope$workspace, scope$project, scope$env, object_type, id)
  )

  for (tg in unique(as.character(unlist(tags)))) {
    if (!nzchar(tg)) {
      next
    }
    DBI::dbExecute(
      con,
      "
      INSERT INTO tags (workspace, project, env, object_type, object_id, tag, created_at)
      VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ",
      params = list(scope$workspace, scope$project, scope$env, object_type, id, tg)
    )
  }

  if (identical(object_type, "automation")) {
    deps <- wf_json_decode(version_row$dependencies_json[[1]])

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
      params = list(scope$workspace, scope$project, scope$env, id)
    )

    if (is.data.frame(deps) && nrow(deps) > 0) {
      for (i in seq_len(nrow(deps))) {
        DBI::dbExecute(
          con,
          "
          INSERT INTO dependencies (
            workspace, project, env, source_type, source_id, target_type, target_id, required, created_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
          ",
          params = list(
            scope$workspace,
            scope$project,
            scope$env,
            "automation",
            id,
            as.character(deps$target_type[[i]]),
            as.character(deps$target_id[[i]]),
            isTRUE(deps$required[[i]])
          )
        )
      }
    }
  }

  current <- wf_find_object(object_type, id, scope)
  list(status = "rolled_back", object = wf_object_to_response(object_type, current, scope = scope))
}

wf_list_tags <- function(scope, object_type = NULL, object_id = NULL) {
  where <- c("workspace = ?", "project = ?", "env = ?")
  params <- list(scope$workspace, scope$project, scope$env)

  if (!is.null(object_type)) {
    where <- c(where, "object_type = ?")
    params <- c(params, list(object_type))
  }

  if (!is.null(object_id)) {
    where <- c(where, "object_id = ?")
    params <- c(params, list(object_id))
  }

  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT object_type, object_id, tag, created_at
      FROM tags
      WHERE %s
      ORDER BY object_type, object_id, tag
      ",
      paste(where, collapse = " AND ")
    ),
    params = params
  )
}

wf_list_dependencies <- function(scope, source_type = NULL, source_id = NULL) {
  where <- c("workspace = ?", "project = ?", "env = ?")
  params <- list(scope$workspace, scope$project, scope$env)

  if (!is.null(source_type)) {
    where <- c(where, "source_type = ?")
    params <- c(params, list(source_type))
  }

  if (!is.null(source_id)) {
    where <- c(where, "source_id = ?")
    params <- c(params, list(source_id))
  }

  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT source_type, source_id, target_type, target_id, required, created_at
      FROM dependencies
      WHERE %s
      ORDER BY source_type, source_id, target_type, target_id
      ",
      paste(where, collapse = " AND ")
    ),
    params = params
  )
}

wf_catalog_search <- function(scope, q = NULL, type = NULL, tag = NULL, sort_by = "updated_at", order = "desc", limit = 100, offset = 0) {
  sort_by <- wf_sort_field(sort_by, allowed = c("object_type", "object_id", "title", "updated_at", "published_version"), default = "updated_at")
  order <- wf_sort_order(order, default = "desc")
  limit <- wf_clamp_int(limit, default = 100L, min_value = 1L, max_value = 1000L)
  offset <- wf_clamp_int(offset, default = 0L, min_value = 0L, max_value = 100000L)

  q <- trimws(as.character(q %||% ""))
  tag <- trimws(as.character(tag %||% ""))
  has_q <- nzchar(q)
  has_tag <- nzchar(tag)
  object_type <- wf_object_type(type)

  where <- character()
  params <- list()

  if (!is.null(object_type)) {
    where <- c(where, "o.object_type = ?")
    params <- c(params, list(object_type))
  }
  if (has_q) {
    where <- c(where, "(LOWER(o.object_id) LIKE LOWER(?) OR LOWER(COALESCE(o.title, '')) LIKE LOWER(?))")
    params <- c(params, list(paste0("%", q, "%"), paste0("%", q, "%")))
  }
  if (has_tag) {
    where <- c(
      where,
      "EXISTS (
        SELECT 1
        FROM tags t
        WHERE t.workspace = o.workspace
          AND t.project = o.project
          AND t.env = o.env
          AND t.object_type = o.object_type
          AND t.object_id = o.object_id
          AND t.tag = ?
      )"
    )
    params <- c(params, list(tag))
  }

  where_sql <- if (length(where) == 0) "" else paste0("WHERE ", paste(where, collapse = " AND "))

  base_sql <- "
    WITH objects AS (
      SELECT workspace, project, env, 'context' AS object_type, context_id AS object_id, title, updated_at, published_version
      FROM contexts
      WHERE workspace = ? AND project = ? AND env = ?
      UNION ALL
      SELECT workspace, project, env, 'skill' AS object_type, skill_id AS object_id, title, updated_at, published_version
      FROM skills
      WHERE workspace = ? AND project = ? AND env = ?
      UNION ALL
      SELECT workspace, project, env, 'automation' AS object_type, automation_id AS object_id, title, updated_at, published_version
      FROM automations
      WHERE workspace = ? AND project = ? AND env = ?
    )
  "

  scope_params <- list(
    scope$workspace,
    scope$project,
    scope$env,
    scope$workspace,
    scope$project,
    scope$env,
    scope$workspace,
    scope$project,
    scope$env
  )

  total <- DBI::dbGetQuery(
    con,
    paste0(base_sql, " SELECT COUNT(*) AS n FROM objects o ", where_sql),
    params = c(scope_params, params)
  )

  rows <- DBI::dbGetQuery(
    con,
    paste0(
      base_sql,
      "
      SELECT o.object_type, o.object_id, o.title, o.updated_at, o.published_version
      FROM objects o
      ",
      where_sql,
      sprintf(
        "
        ORDER BY o.%s %s, o.object_type ASC, o.object_id ASC
        LIMIT %d
        OFFSET %d
        ",
        sort_by,
        order,
        limit,
        offset
      )
    ),
    params = c(scope_params, params)
  )

  items <- lapply(
    seq_len(nrow(rows)),
    function(i) {
      row <- rows[i, , drop = FALSE]
      obj_type <- as.character(row$object_type[[1]])
      obj_id <- as.character(row$object_id[[1]])
      list(
        object_type = obj_type,
        object_id = obj_id,
        title = as.character(row$title[[1]] %||% ""),
        updated_at = as.character(row$updated_at[[1]]),
        published_version = as.integer(row$published_version[[1]] %||% 0L),
        tags = wf_get_tags(obj_type, obj_id, scope)
      )
    }
  )

  list(
    items = items,
    total = as.integer(total$n[[1]]),
    limit = limit,
    offset = offset
  )
}

wf_catalog_tree <- function(scope, group_by = "type") {
  mode <- tolower(trimws(as.character(group_by %||% "type")))
  if (!(mode %in% c("type", "tag"))) {
    mode <- "type"
  }

  all_items <- wf_catalog_search(
    scope = scope,
    q = NULL,
    type = NULL,
    tag = NULL,
    sort_by = "updated_at",
    order = "desc",
    limit = 100000L,
    offset = 0L
  )$items

  if (identical(mode, "type")) {
    contexts <- all_items[vapply(all_items, function(x) identical(x$object_type, "context"), logical(1))]
    skills <- all_items[vapply(all_items, function(x) identical(x$object_type, "skill"), logical(1))]
    automations <- all_items[vapply(all_items, function(x) identical(x$object_type, "automation"), logical(1))]

    return(
      list(
        group_by = "type",
        groups = list(
          contexts = contexts,
          skills = skills,
          automations = automations
        )
      )
    )
  }

  tag_rows <- wf_list_tags(scope = scope)
  groups <- list()
  if (nrow(tag_rows) > 0) {
    for (i in seq_len(nrow(tag_rows))) {
      tg <- as.character(tag_rows$tag[[i]])
      if (is.null(groups[[tg]])) {
        groups[[tg]] <- list()
      }
      groups[[tg]][[length(groups[[tg]]) + 1L]] <- list(
        object_type = as.character(tag_rows$object_type[[i]]),
        object_id = as.character(tag_rows$object_id[[i]])
      )
    }
  }

  list(group_by = "tag", groups = groups)
}

wf_catalog_resolve <- function(res, req) {
  parsed <- wf_json_parse(req, res)
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  refs <- parsed$refs
  if (is.null(refs) || !is.list(refs) || length(refs) == 0) {
    res$status <- 400
    return(list(error = "Field 'refs' must be a non-empty list"))
  }

  scope <- wf_scope(parsed$workspace %||% "personal", parsed$project %||% "default", parsed$env %||% "dev")
  include_dependencies <- isTRUE(parsed$include_dependencies %||% TRUE)

  resolved <- list()
  missing <- list()

  add_missing <- function(obj_type, obj_id) {
    missing[[length(missing) + 1L]] <<- list(object_type = obj_type, object_id = obj_id)
  }

  for (ref in refs) {
    obj_type <- wf_object_type(ref$object_type %||% "")
    obj_id <- wf_id(ref$object_id %||% "")
    if (is.null(obj_type) || is.null(obj_id)) {
      add_missing(ref$object_type, ref$object_id)
      next
    }

    row <- wf_find_object(obj_type, obj_id, scope)
    if (is.null(row)) {
      add_missing(obj_type, obj_id)
      next
    }

    resolved[[length(resolved) + 1L]] <- wf_object_to_response(obj_type, row, scope = scope)

    if (include_dependencies && identical(obj_type, "automation")) {
      deps <- wf_get_dependencies("automation", obj_id, scope)
      if (nrow(deps) > 0) {
        for (i in seq_len(nrow(deps))) {
          tgt_type <- wf_object_type(as.character(deps$target_type[[i]]))
          tgt_id <- wf_id(as.character(deps$target_id[[i]]))
          if (is.null(tgt_type) || is.null(tgt_id)) {
            next
          }
          tgt_row <- wf_find_object(tgt_type, tgt_id, scope)
          if (is.null(tgt_row)) {
            add_missing(tgt_type, tgt_id)
          } else {
            resolved[[length(resolved) + 1L]] <- wf_object_to_response(tgt_type, tgt_row, scope = scope)
          }
        }
      }
    }
  }

  dedupe <- function(items) {
    out <- list()
    seen <- character()
    for (it in items) {
      key <- paste0(as.character(it$object_type %||% ""), ":", as.character(it$object_id %||% ""))
      if (key %in% seen) {
        next
      }
      seen <- c(seen, key)
      out[[length(out) + 1L]] <- it
    }
    out
  }

  list(
    resolved = dedupe(resolved),
    missing = dedupe(missing)
  )
}

#* List contexts.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/contexts
function(res, workspace = "personal", project = "default", env = "dev", tag = NULL, q = NULL, sort_by = "updated_at", order = "desc", limit = 100, offset = 0) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_list_objects("context", scope = wf_scope(workspace, project, env), tag = tag, q = q, sort_by = sort_by, order = order, limit = limit, offset = offset)
}

#* Get one context by id.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/contexts/<context_id>
function(res, context_id, workspace = "personal", project = "default", env = "dev", version = NULL) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_get_object(res, "context", context_id, wf_scope(workspace, project, env), version = version)
}

#* Upsert context draft state.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @put /v1/contexts/<context_id>
function(req, res, context_id, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_upsert_object(res, req, "context", context_id, wf_scope(workspace, project, env))
}

#* Publish context to a new immutable version.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/contexts/<context_id>/publish
function(req, res, context_id, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_publish_object(res, req, "context", context_id, wf_scope(workspace, project, env))
}

#* List context versions.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/contexts/<context_id>/versions
function(res, context_id, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_list_versions("context", context_id, wf_scope(workspace, project, env))
}

#* Roll back context draft state to a prior version.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/contexts/<context_id>/rollback
function(req, res, context_id, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_rollback(res, req, "context", context_id, wf_scope(workspace, project, env))
}

#* Delete one context and clean related tags/dependencies.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @delete /v1/contexts/<context_id>
function(res, context_id, workspace = "personal", project = "default", env = "dev", delete_versions = TRUE) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_delete_object(
    res = res,
    object_type = "context",
    object_id = context_id,
    scope = wf_scope(workspace, project, env),
    delete_versions = wf_bool(delete_versions, default = TRUE)
  )
}

#* List skills.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/skills
function(res, workspace = "personal", project = "default", env = "dev", tag = NULL, q = NULL, sort_by = "updated_at", order = "desc", limit = 100, offset = 0) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_list_objects("skill", scope = wf_scope(workspace, project, env), tag = tag, q = q, sort_by = sort_by, order = order, limit = limit, offset = offset)
}

#* Get one skill by id.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/skills/<skill_id>
function(res, skill_id, workspace = "personal", project = "default", env = "dev", version = NULL) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_get_object(res, "skill", skill_id, wf_scope(workspace, project, env), version = version)
}

#* Upsert skill draft state.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @put /v1/skills/<skill_id>
function(req, res, skill_id, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_upsert_object(res, req, "skill", skill_id, wf_scope(workspace, project, env))
}

#* Publish skill to a new immutable version.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/skills/<skill_id>/publish
function(req, res, skill_id, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_publish_object(res, req, "skill", skill_id, wf_scope(workspace, project, env))
}

#* List skill versions.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/skills/<skill_id>/versions
function(res, skill_id, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_list_versions("skill", skill_id, wf_scope(workspace, project, env))
}

#* Roll back skill draft state to a prior version.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/skills/<skill_id>/rollback
function(req, res, skill_id, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_rollback(res, req, "skill", skill_id, wf_scope(workspace, project, env))
}

#* Delete one skill and clean related tags/dependencies.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @delete /v1/skills/<skill_id>
function(res, skill_id, workspace = "personal", project = "default", env = "dev", delete_versions = TRUE) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_delete_object(
    res = res,
    object_type = "skill",
    object_id = skill_id,
    scope = wf_scope(workspace, project, env),
    delete_versions = wf_bool(delete_versions, default = TRUE)
  )
}

#* List automations.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/automations
function(res, workspace = "personal", project = "default", env = "dev", tag = NULL, q = NULL, sort_by = "updated_at", order = "desc", limit = 100, offset = 0) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_list_objects("automation", scope = wf_scope(workspace, project, env), tag = tag, q = q, sort_by = sort_by, order = order, limit = limit, offset = offset)
}

#* Get one automation by id.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/automations/<automation_id>
function(res, automation_id, workspace = "personal", project = "default", env = "dev", version = NULL) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_get_object(res, "automation", automation_id, wf_scope(workspace, project, env), version = version)
}

#* Upsert automation draft state.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @put /v1/automations/<automation_id>
function(req, res, automation_id, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_upsert_object(res, req, "automation", automation_id, wf_scope(workspace, project, env))
}

#* Publish automation to a new immutable version.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/automations/<automation_id>/publish
function(req, res, automation_id, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_publish_object(res, req, "automation", automation_id, wf_scope(workspace, project, env))
}

#* List automation versions.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/automations/<automation_id>/versions
function(res, automation_id, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_list_versions("automation", automation_id, wf_scope(workspace, project, env))
}

#* Roll back automation draft state to a prior version.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/automations/<automation_id>/rollback
function(req, res, automation_id, workspace = "personal", project = "default", env = "dev") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_rollback(res, req, "automation", automation_id, wf_scope(workspace, project, env))
}

#* Delete one automation and clean related tags/dependencies.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @delete /v1/automations/<automation_id>
function(res, automation_id, workspace = "personal", project = "default", env = "dev", delete_versions = TRUE) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_delete_object(
    res = res,
    object_type = "automation",
    object_id = automation_id,
    scope = wf_scope(workspace, project, env),
    delete_versions = wf_bool(delete_versions, default = TRUE)
  )
}

#* Replace tags for an object.
#* Body fields: `object_type`, `object_id`, `tags` (array).
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @put /v1/tags
function(req, res) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  parsed <- wf_json_parse(req, res)
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  object_type <- wf_object_type(parsed$object_type)
  object_id <- wf_id(parsed$object_id)
  scope <- wf_scope(parsed$workspace %||% "personal", parsed$project %||% "default", parsed$env %||% "dev")

  if (is.null(object_type) || is.null(object_id)) {
    res$status <- 400
    return(list(error = "Fields 'object_type' and 'object_id' are required"))
  }

  if (is.null(wf_find_object(object_type, object_id, scope))) {
    res$status <- 404
    return(list(error = "Object not found"))
  }

  tags <- parsed$tags %||% character()
  tags <- unique(trimws(as.character(unlist(tags))))
  tags <- tags[nzchar(tags)]

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
    params = list(scope$workspace, scope$project, scope$env, object_type, object_id)
  )

  for (tag in tags) {
    DBI::dbExecute(
      con,
      "
      INSERT INTO tags (workspace, project, env, object_type, object_id, tag, created_at)
      VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ",
      params = list(scope$workspace, scope$project, scope$env, object_type, object_id, tag)
    )
  }

  list(status = "saved", object_type = object_type, object_id = object_id, tags = tags)
}

#* List tags.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/tags
function(res, workspace = "personal", project = "default", env = "dev", object_type = NULL, object_id = NULL) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  obj_type <- NULL
  if (!is.null(object_type) && nzchar(trimws(as.character(object_type)))) {
    obj_type <- wf_object_type(object_type)
    if (is.null(obj_type)) {
      res$status <- 400
      return(list(error = "Invalid object_type"))
    }
  }

  obj_id <- NULL
  if (!is.null(object_id) && nzchar(trimws(as.character(object_id)))) {
    obj_id <- wf_id(object_id)
  }

  wf_list_tags(scope = wf_scope(workspace, project, env), object_type = obj_type, object_id = obj_id)
}

#* Delete tags for an object, optionally a specific tag.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @delete /v1/tags
function(res, workspace = "personal", project = "default", env = "dev", object_type = NULL, object_id = NULL, tag = NULL) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  obj_type <- wf_object_type(object_type)
  obj_id <- wf_id(object_id)
  tg <- wf_id(tag)

  if (is.null(obj_type) || is.null(obj_id)) {
    res$status <- 400
    return(list(error = "Query params 'object_type' and 'object_id' are required"))
  }

  where <- c("workspace = ?", "project = ?", "env = ?", "object_type = ?", "object_id = ?")
  params <- list(workspace, project, env, obj_type, obj_id)
  if (!is.null(tg)) {
    where <- c(where, "tag = ?")
    params <- c(params, list(tg))
  }

  DBI::dbExecute(
    con,
    sprintf("DELETE FROM tags WHERE %s", paste(where, collapse = " AND ")),
    params = params
  )

  list(status = "deleted", object_type = obj_type, object_id = obj_id, tag = tg)
}

#* Upsert one dependency edge.
#* Body fields: `source_type`, `source_id`, `target_type`, `target_id`.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @put /v1/dependencies
function(req, res) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  parsed <- wf_json_parse(req, res)
  if (!is.list(parsed) || !is.null(parsed$error)) {
    return(parsed)
  }

  scope <- wf_scope(parsed$workspace %||% "personal", parsed$project %||% "default", parsed$env %||% "dev")
  source_type <- wf_object_type(parsed$source_type)
  source_id <- wf_id(parsed$source_id)
  target_type <- wf_object_type(parsed$target_type)
  target_id <- wf_id(parsed$target_id)

  if (is.null(source_type) || is.null(source_id) || is.null(target_type) || is.null(target_id)) {
    res$status <- 400
    return(list(error = "Fields source_type/source_id/target_type/target_id are required"))
  }

  if (is.null(wf_find_object(source_type, source_id, scope))) {
    res$status <- 404
    return(list(error = "Source object not found"))
  }
  if (is.null(wf_find_object(target_type, target_id, scope))) {
    res$status <- 404
    return(list(error = "Target object not found"))
  }

  required <- isTRUE(parsed$required %||% TRUE)

  DBI::dbExecute(
    con,
    "
    INSERT INTO dependencies (
      workspace, project, env, source_type, source_id, target_type, target_id, required, created_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT (workspace, project, env, source_type, source_id, target_type, target_id)
    DO UPDATE SET required = EXCLUDED.required
    ",
    params = list(scope$workspace, scope$project, scope$env, source_type, source_id, target_type, target_id, required)
  )

  list(status = "saved")
}

#* List dependency edges.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/dependencies
function(res, workspace = "personal", project = "default", env = "dev", source_type = NULL, source_id = NULL) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  src_type <- NULL
  if (!is.null(source_type) && nzchar(trimws(as.character(source_type)))) {
    src_type <- wf_object_type(source_type)
    if (is.null(src_type)) {
      res$status <- 400
      return(list(error = "Invalid source_type"))
    }
  }

  src_id <- NULL
  if (!is.null(source_id) && nzchar(trimws(as.character(source_id)))) {
    src_id <- wf_id(source_id)
  }

  wf_list_dependencies(scope = wf_scope(workspace, project, env), source_type = src_type, source_id = src_id)
}

#* Delete one dependency edge.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @delete /v1/dependencies
function(res, workspace = "personal", project = "default", env = "dev", source_type = NULL, source_id = NULL, target_type = NULL, target_id = NULL) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  src_type <- wf_object_type(source_type)
  src_id <- wf_id(source_id)
  tgt_type <- wf_object_type(target_type)
  tgt_id <- wf_id(target_id)

  if (is.null(src_type) || is.null(src_id) || is.null(tgt_type) || is.null(tgt_id)) {
    res$status <- 400
    return(list(error = "Query params source_type/source_id/target_type/target_id are required"))
  }

  DBI::dbExecute(
    con,
    "
    DELETE FROM dependencies
    WHERE workspace = ?
      AND project = ?
      AND env = ?
      AND source_type = ?
      AND source_id = ?
      AND target_type = ?
      AND target_id = ?
    ",
    params = list(workspace, project, env, src_type, src_id, tgt_type, tgt_id)
  )

  list(status = "deleted")
}

#* Search contexts, skills, and automations with unified filters.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/catalog/search
function(res, workspace = "personal", project = "default", env = "dev", q = NULL, type = NULL, tag = NULL, sort_by = "updated_at", order = "desc", limit = 100, offset = 0) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_catalog_search(
    scope = wf_scope(workspace, project, env),
    q = q,
    type = type,
    tag = tag,
    sort_by = sort_by,
    order = order,
    limit = limit,
    offset = offset
  )
}

#* Return catalog grouped by type or tag.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/catalog/tree
function(res, workspace = "personal", project = "default", env = "dev", group_by = "type") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_catalog_tree(scope = wf_scope(workspace, project, env), group_by = group_by)
}

#* Resolve explicit refs into full current objects.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/catalog/resolve
function(req, res) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_catalog_resolve(res, req)
}

wf_to_character_vector <- function(x) {
  if (is.null(x)) {
    return(character())
  }
  as.character(unlist(x))
}

wf_lint_skill_object <- function(obj) {
  issues <- character()
  warnings <- character()

  title <- trimws(as.character(obj$title %||% ""))
  if (!nzchar(title)) {
    warnings <- c(warnings, "Skill title is empty.")
  }

  content <- obj$content
  if (is.null(content) || !(is.list(content) || is.atomic(content))) {
    issues <- c(issues, "Skill content is missing or invalid.")
  } else if (!is.list(content)) {
    warnings <- c(warnings, "Skill content is not an object; prefer object-shaped JSON.")
  }

  if (is.list(content) && !is.null(content$prompt)) {
    prompt <- trimws(as.character(content$prompt %||% ""))
    if (!nzchar(prompt)) {
      warnings <- c(warnings, "Skill prompt is present but empty.")
    }
  }

  list(
    valid = length(issues) == 0L,
    issues = issues,
    warnings = warnings
  )
}

wf_validate_automation_object <- function(obj, scope) {
  issues <- character()
  warnings <- character()

  trigger_type <- trimws(as.character(obj$trigger_type %||% ""))
  if (!nzchar(trigger_type)) {
    warnings <- c(warnings, "Automation trigger_type is empty.")
  }

  content <- obj$content
  if (is.null(content) || !(is.list(content) || is.atomic(content))) {
    issues <- c(issues, "Automation content is missing or invalid.")
  } else if (!is.list(content)) {
    warnings <- c(warnings, "Automation content is not an object; prefer object-shaped JSON.")
  }

  deps <- obj$dependencies
  if (!is.data.frame(deps)) {
    deps <- data.frame(
      source_type = character(),
      source_id = character(),
      target_type = character(),
      target_id = character(),
      required = logical(),
      stringsAsFactors = FALSE
    )
  }

  missing_dependencies <- list()

  if (nrow(deps) > 0) {
    for (i in seq_len(nrow(deps))) {
      target_type <- wf_object_type(as.character(deps$target_type[[i]] %||% ""))
      target_id <- wf_id(as.character(deps$target_id[[i]] %||% ""))
      required <- isTRUE(deps$required[[i]] %||% TRUE)

      if (is.null(target_type) || is.null(target_id)) {
        missing_dependencies[[length(missing_dependencies) + 1L]] <- list(
          target_type = deps$target_type[[i]],
          target_id = deps$target_id[[i]],
          required = required,
          reason = "invalid_target_reference"
        )
        issues <- c(issues, "Automation contains dependency with invalid target reference.")
        next
      }

      target_row <- wf_find_object(target_type, target_id, scope)
      if (is.null(target_row)) {
        missing_dependencies[[length(missing_dependencies) + 1L]] <- list(
          target_type = target_type,
          target_id = target_id,
          required = required,
          reason = "target_not_found"
        )
        if (required) {
          issues <- c(issues, paste0("Missing required dependency: ", target_type, ":", target_id))
        } else {
          warnings <- c(warnings, paste0("Missing optional dependency: ", target_type, ":", target_id))
        }
      }
    }
  }

  list(
    valid = length(issues) == 0L,
    issues = unique(issues),
    warnings = unique(warnings),
    dependency_count = nrow(deps),
    missing_dependencies = missing_dependencies
  )
}

wf_ref_integrity <- function(scope, limit = 500L) {
  limit <- wf_clamp_int(limit, default = 500L, min_value = 1L, max_value = 5000L)

  dependency_rows <- wf_list_dependencies(scope = scope)
  tag_rows <- wf_list_tags(scope = scope)

  broken_dependencies <- list()
  broken_tags <- list()

  if (nrow(dependency_rows) > 0) {
    for (i in seq_len(nrow(dependency_rows))) {
      src_type <- wf_object_type(as.character(dependency_rows$source_type[[i]] %||% ""))
      src_id <- wf_id(as.character(dependency_rows$source_id[[i]] %||% ""))
      tgt_type <- wf_object_type(as.character(dependency_rows$target_type[[i]] %||% ""))
      tgt_id <- wf_id(as.character(dependency_rows$target_id[[i]] %||% ""))

      source_exists <- !is.null(src_type) && !is.null(src_id) && !is.null(wf_find_object(src_type, src_id, scope))
      target_exists <- !is.null(tgt_type) && !is.null(tgt_id) && !is.null(wf_find_object(tgt_type, tgt_id, scope))

      if (!source_exists || !target_exists) {
        broken_dependencies[[length(broken_dependencies) + 1L]] <- list(
          source_type = dependency_rows$source_type[[i]],
          source_id = dependency_rows$source_id[[i]],
          target_type = dependency_rows$target_type[[i]],
          target_id = dependency_rows$target_id[[i]],
          source_exists = source_exists,
          target_exists = target_exists
        )
      }
    }
  }

  if (nrow(tag_rows) > 0) {
    for (i in seq_len(nrow(tag_rows))) {
      obj_type <- wf_object_type(as.character(tag_rows$object_type[[i]] %||% ""))
      obj_id <- wf_id(as.character(tag_rows$object_id[[i]] %||% ""))
      exists <- !is.null(obj_type) && !is.null(obj_id) && !is.null(wf_find_object(obj_type, obj_id, scope))
      if (!exists) {
        broken_tags[[length(broken_tags) + 1L]] <- list(
          object_type = tag_rows$object_type[[i]],
          object_id = tag_rows$object_id[[i]],
          tag = tag_rows$tag[[i]]
        )
      }
    }
  }

  list(
    valid = length(broken_dependencies) == 0L && length(broken_tags) == 0L,
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    totals = list(
      dependencies = nrow(dependency_rows),
      tags = nrow(tag_rows)
    ),
    broken_counts = list(
      dependencies = length(broken_dependencies),
      tags = length(broken_tags)
    ),
    broken_dependencies = utils::head(broken_dependencies, n = limit),
    broken_tags = utils::head(broken_tags, n = limit)
  )
}

#* Lint a skill draft/version for basic structural issues.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/skills/<skill_id>/lint
function(res, skill_id, workspace = "personal", project = "default", env = "dev", version = NULL) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  scope <- wf_scope(workspace, project, env)
  obj <- wf_get_object(res, "skill", skill_id, scope, version = version)
  if (!is.null(obj$error)) {
    return(obj)
  }

  report <- wf_lint_skill_object(obj)
  list(
    object_type = "skill",
    object_id = skill_id,
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    version = obj$version %||% NULL,
    valid = report$valid,
    issues = report$issues,
    warnings = report$warnings
  )
}

#* Validate automation draft/version for structural and dependency integrity.
#* @tag Workflows
#* @serializer json list(auto_unbox = TRUE)
#* @post /v1/automations/<automation_id>/validate
function(res, automation_id, workspace = "personal", project = "default", env = "dev", version = NULL) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  scope <- wf_scope(workspace, project, env)
  obj <- wf_get_object(res, "automation", automation_id, scope, version = version)
  if (!is.null(obj$error)) {
    return(obj)
  }

  report <- wf_validate_automation_object(obj, scope)
  list(
    object_type = "automation",
    object_id = automation_id,
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    version = obj$version %||% NULL,
    valid = report$valid,
    issues = report$issues,
    warnings = report$warnings,
    dependency_count = report$dependency_count,
    missing_dependencies = report$missing_dependencies
  )
}

#* Validate cross-object reference integrity for tags and dependencies in scope.
#* @tag Workflows
#* @param limit:int Maximum broken refs returned per category (1..5000).
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/validate/ref-integrity
function(res, workspace = "personal", project = "default", env = "dev", limit = 500) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  wf_ref_integrity(wf_scope(workspace, project, env), limit = limit)
}
