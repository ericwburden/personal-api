#* Workflow activity endpoints for cross-surface event timelines.
#* Requires bearer authentication.

wf_activity_allowed_event_types <- c("draft_saved", "published", "run_started", "run_finished", "snapshot_created")
wf_activity_allowed_object_types <- c("context", "skill", "automation", "snapshot")

wf_activity_table_exists <- function(table_name) {
  isTRUE(DBI::dbExistsTable(con, table_name))
}

wf_activity_empty_frame <- function() {
  data.frame(
    event_type = character(),
    event_id = character(),
    object_type = character(),
    object_id = character(),
    actor = character(),
    status = character(),
    note = character(),
    version = character(),
    run_id = character(),
    event_at = as.POSIXct(character()),
    stringsAsFactors = FALSE
  )
}

wf_activity_parse_time <- function(res, value, label) {
  raw <- trimws(as.character(value %||% ""))
  if (!nzchar(raw)) {
    return(NULL)
  }

  parsed <- suppressWarnings(as.POSIXct(raw, tz = "UTC"))
  if (is.na(parsed)) {
    res$status <- 400
    return(list(error = paste0("Invalid '", label, "' timestamp. Use ISO-8601 UTC format.")))
  }

  parsed
}

wf_activity_parse_event_types <- function(res, event_type) {
  raw <- wf_to_character_vector(event_type)
  raw <- trimws(tolower(raw))
  raw <- raw[nzchar(raw)]

  if (length(raw) == 1L && grepl(",", raw[[1]], fixed = TRUE)) {
    raw <- trimws(unlist(strsplit(raw[[1]], ",", fixed = TRUE), use.names = FALSE))
    raw <- raw[nzchar(raw)]
  }

  if (length(raw) == 0L) {
    return(NULL)
  }

  out <- unique(raw)
  invalid <- out[!(out %in% wf_activity_allowed_event_types)]
  if (length(invalid) > 0L) {
    res$status <- 400
    return(list(error = paste0("Invalid event_type values: ", paste(invalid, collapse = ", "))))
  }

  out
}

wf_activity_parse_object_type <- function(res, object_type) {
  value <- trimws(tolower(as.character(object_type %||% "")))
  if (!nzchar(value)) {
    return(NULL)
  }

  if (!(value %in% wf_activity_allowed_object_types)) {
    res$status <- 400
    return(list(error = "Invalid object_type. Expected context, skill, automation, or snapshot."))
  }

  value
}

wf_activity_allow <- function(filter_values, value) {
  is.null(filter_values) || value %in% filter_values
}

wf_activity_time_where <- function(column_name, from_time, to_time) {
  where <- character()
  params <- list()

  if (!is.null(from_time)) {
    where <- c(where, paste0(column_name, " >= ?"))
    params <- c(params, list(from_time))
  }

  if (!is.null(to_time)) {
    where <- c(where, paste0(column_name, " <= ?"))
    params <- c(params, list(to_time))
  }

  list(where = where, params = params)
}

wf_activity_collect_rows <- function(scope, event_types = NULL, object_type = NULL, object_id = NULL, from_time = NULL, to_time = NULL) {
  rows <- list()

  add_rows <- function(df) {
    if (is.data.frame(df) && nrow(df) > 0) {
      rows[[length(rows) + 1L]] <<- df
    }
  }

  object_tables <- list(
    context = list(table = "contexts", id_col = "context_id"),
    skill = list(table = "skills", id_col = "skill_id"),
    automation = list(table = "automations", id_col = "automation_id")
  )

  if (wf_activity_allow(event_types, "draft_saved")) {
    for (obj_type in names(object_tables)) {
      if (!is.null(object_type) && !identical(object_type, obj_type)) {
        next
      }

      spec <- object_tables[[obj_type]]
      if (!wf_activity_table_exists(spec$table)) {
        next
      }

      where <- c("workspace = ?", "project = ?", "env = ?")
      params <- list(scope$workspace, scope$project, scope$env)

      if (!is.null(object_id)) {
        where <- c(where, paste0(spec$id_col, " = ?"))
        params <- c(params, list(object_id))
      }

      tw <- wf_activity_time_where("updated_at", from_time, to_time)
      where <- c(where, tw$where)
      params <- c(params, tw$params)

      out <- DBI::dbGetQuery(
        con,
        sprintf(
          "
          SELECT
            'draft_saved' AS event_type,
            CONCAT('%s:', %s, ':draft') AS event_id,
            '%s' AS object_type,
            %s AS object_id,
            COALESCE(updated_by, '') AS actor,
            '' AS status,
            COALESCE(title, '') AS note,
            '' AS version,
            '' AS run_id,
            updated_at AS event_at
          FROM %s
          WHERE %s
          ",
          obj_type,
          spec$id_col,
          obj_type,
          spec$id_col,
          spec$table,
          paste(where, collapse = " AND ")
        ),
        params = params
      )

      add_rows(out)
    }
  }

  if (wf_activity_allow(event_types, "published") && wf_activity_table_exists("versions")) {
    where <- c("workspace = ?", "project = ?", "env = ?")
    params <- list(scope$workspace, scope$project, scope$env)

    if (!is.null(object_type) && !identical(object_type, "snapshot")) {
      where <- c(where, "object_type = ?")
      params <- c(params, list(object_type))
    }

    if (!is.null(object_id)) {
      where <- c(where, "object_id = ?")
      params <- c(params, list(object_id))
    }

    tw <- wf_activity_time_where("created_at", from_time, to_time)
    where <- c(where, tw$where)
    params <- c(params, tw$params)

    out <- DBI::dbGetQuery(
      con,
      sprintf(
        "
        SELECT
          'published' AS event_type,
          CONCAT(object_type, ':', object_id, ':v', CAST(version AS VARCHAR)) AS event_id,
          object_type,
          object_id,
          COALESCE(created_by, '') AS actor,
          '' AS status,
          COALESCE(change_note, '') AS note,
          CAST(version AS VARCHAR) AS version,
          '' AS run_id,
          created_at AS event_at
        FROM versions
        WHERE %s
        ",
        paste(where, collapse = " AND ")
      ),
      params = params
    )

    add_rows(out)
  }

  if ((wf_activity_allow(event_types, "run_started") || wf_activity_allow(event_types, "run_finished")) && wf_activity_table_exists("automation_runs")) {
    include_runs <- is.null(object_type) || identical(object_type, "automation")

    if (isTRUE(include_runs)) {
      where_common <- c("workspace = ?", "project = ?", "env = ?")
      params_common <- list(scope$workspace, scope$project, scope$env)

      if (!is.null(object_id)) {
        where_common <- c(where_common, "automation_id = ?")
        params_common <- c(params_common, list(object_id))
      }

      if (wf_activity_allow(event_types, "run_started")) {
        where <- where_common
        params <- params_common

        tw <- wf_activity_time_where("started_at", from_time, to_time)
        where <- c(where, tw$where)
        params <- c(params, tw$params)

        out <- DBI::dbGetQuery(
          con,
          sprintf(
            "
            SELECT
              'run_started' AS event_type,
              CONCAT(run_id, ':started') AS event_id,
              'automation' AS object_type,
              automation_id AS object_id,
              COALESCE(requested_by, '') AS actor,
              'running' AS status,
              '' AS note,
              '' AS version,
              run_id,
              started_at AS event_at
            FROM automation_runs
            WHERE %s
            ",
            paste(where, collapse = " AND ")
          ),
          params = params
        )

        add_rows(out)
      }

      if (wf_activity_allow(event_types, "run_finished")) {
        where <- c(where_common, "finished_at IS NOT NULL")
        params <- params_common

        tw <- wf_activity_time_where("finished_at", from_time, to_time)
        where <- c(where, tw$where)
        params <- c(params, tw$params)

        out <- DBI::dbGetQuery(
          con,
          sprintf(
            "
            SELECT
              'run_finished' AS event_type,
              CONCAT(run_id, ':finished') AS event_id,
              'automation' AS object_type,
              automation_id AS object_id,
              COALESCE(requested_by, '') AS actor,
              COALESCE(status, '') AS status,
              COALESCE(error_text, '') AS note,
              '' AS version,
              run_id,
              finished_at AS event_at
            FROM automation_runs
            WHERE %s
            ",
            paste(where, collapse = " AND ")
          ),
          params = params
        )

        add_rows(out)
      }
    }
  }

  if (wf_activity_allow(event_types, "snapshot_created") && wf_activity_table_exists("snapshots")) {
    include_snapshots <- is.null(object_type) || identical(object_type, "snapshot")

    if (isTRUE(include_snapshots)) {
      where <- c("workspace = ?", "project = ?", "env = ?")
      params <- list(scope$workspace, scope$project, scope$env)

      if (!is.null(object_id)) {
        where <- c(where, "snapshot_id = ?")
        params <- c(params, list(object_id))
      }

      tw <- wf_activity_time_where("created_at", from_time, to_time)
      where <- c(where, tw$where)
      params <- c(params, tw$params)

      out <- DBI::dbGetQuery(
        con,
        sprintf(
          "
          SELECT
            'snapshot_created' AS event_type,
            snapshot_id AS event_id,
            'snapshot' AS object_type,
            snapshot_id AS object_id,
            COALESCE(created_by, '') AS actor,
            '' AS status,
            COALESCE(note, '') AS note,
            '' AS version,
            '' AS run_id,
            created_at AS event_at
          FROM snapshots
          WHERE %s
          ",
          paste(where, collapse = " AND ")
        ),
        params = params
      )

      add_rows(out)
    }
  }

  if (length(rows) == 0L) {
    return(wf_activity_empty_frame())
  }

  do.call(rbind, rows)
}

wf_activity_timestamp_utc <- function(x) {
  parsed <- suppressWarnings(as.POSIXct(x, tz = "UTC"))
  if (is.na(parsed)) {
    return("")
  }
  format(parsed, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}

#* Return workflow activity feed across objects, runs, and snapshots.
#* @tag Workflows
#* @param event_type Optional filter (`draft_saved,published,run_started,run_finished,snapshot_created`). Accepts CSV.
#* @param object_type Optional filter (`context|skill|automation|snapshot`).
#* @param object_id Optional filter for one object id.
#* @param from Optional lower-bound UTC timestamp.
#* @param to Optional upper-bound UTC timestamp.
#* @param limit:int Maximum rows (1..2000).
#* @param offset:int Rows to skip.
#* @param order Sort order (`asc|desc`) by event time.
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/workflows/activity
function(res, workspace = "personal", project = "default", env = "dev", event_type = NULL, object_type = NULL, object_id = NULL, from = NULL, to = NULL, limit = 200, offset = 0, order = "desc") {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  scope <- wf_scope(workspace, project, env)
  limit <- wf_clamp_int(limit, default = 200L, min_value = 1L, max_value = 2000L)
  offset <- wf_clamp_int(offset, default = 0L, min_value = 0L, max_value = 100000L)
  order <- wf_sort_order(order, default = "desc")

  event_types <- wf_activity_parse_event_types(res, event_type)
  if (is.list(event_types) && !is.null(event_types$error)) {
    return(event_types)
  }

  obj_type <- wf_activity_parse_object_type(res, object_type)
  if (is.list(obj_type) && !is.null(obj_type$error)) {
    return(obj_type)
  }

  obj_id <- wf_id(object_id)

  from_time <- wf_activity_parse_time(res, from, "from")
  if (is.list(from_time) && !is.null(from_time$error)) {
    return(from_time)
  }

  to_time <- wf_activity_parse_time(res, to, "to")
  if (is.list(to_time) && !is.null(to_time$error)) {
    return(to_time)
  }

  rows <- wf_activity_collect_rows(
    scope = scope,
    event_types = event_types,
    object_type = obj_type,
    object_id = obj_id,
    from_time = from_time,
    to_time = to_time
  )

  if (nrow(rows) > 0) {
    event_at <- suppressWarnings(as.POSIXct(rows$event_at, tz = "UTC"))
    order_index <- order(event_at, rows$event_id, decreasing = identical(order, "desc"), na.last = TRUE)
    rows <- rows[order_index, , drop = FALSE]
  }

  total <- as.integer(nrow(rows))

  if (offset >= total) {
    page <- rows[0, , drop = FALSE]
  } else {
    end_index <- min(offset + limit, total)
    page <- rows[(offset + 1L):end_index, , drop = FALSE]
  }

  items <- lapply(
    seq_len(nrow(page)),
    function(i) {
      row <- page[i, , drop = FALSE]
      list(
        event_type = as.character(row$event_type[[1]] %||% ""),
        event_id = as.character(row$event_id[[1]] %||% ""),
        object_type = as.character(row$object_type[[1]] %||% ""),
        object_id = as.character(row$object_id[[1]] %||% ""),
        timestamp_utc = wf_activity_timestamp_utc(row$event_at[[1]]),
        actor = as.character(row$actor[[1]] %||% ""),
        status = as.character(row$status[[1]] %||% ""),
        note = as.character(row$note[[1]] %||% ""),
        version = as.character(row$version[[1]] %||% ""),
        run_id = as.character(row$run_id[[1]] %||% "")
      )
    }
  )

  list(
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    total = total,
    limit = limit,
    offset = offset,
    order = order,
    items = items
  )
}
