#* Workflow graph endpoints for dependency cycle checks and execution planning.
#* Requires bearer authentication.

wf_graph_key <- function(object_type, object_id) {
  paste0(as.character(object_type %||% ""), ":", as.character(object_id %||% ""))
}

wf_graph_key_to_ref <- function(key) {
  raw <- as.character(key %||% "")
  parts <- strsplit(raw, ":", fixed = TRUE)[[1]]
  if (length(parts) < 2L) {
    return(list(object_type = "", object_id = raw))
  }

  list(
    object_type = parts[[1]],
    object_id = paste(parts[-1], collapse = ":")
  )
}

wf_graph_dependency_rows <- function(scope, source_type = NULL, target_type = NULL, required_only = TRUE) {
  where <- c("workspace = ?", "project = ?", "env = ?")
  params <- list(scope$workspace, scope$project, scope$env)

  if (!is.null(source_type)) {
    where <- c(where, "source_type = ?")
    params <- c(params, list(as.character(source_type)))
  }

  if (!is.null(target_type)) {
    where <- c(where, "target_type = ?")
    params <- c(params, list(as.character(target_type)))
  }

  if (isTRUE(required_only)) {
    where <- c(where, "required = TRUE")
  }

  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT source_type, source_id, target_type, target_id, required
      FROM dependencies
      WHERE %s
      ORDER BY source_type, source_id, target_type, target_id
      ",
      paste(where, collapse = " AND ")
    ),
    params = params
  )
}

wf_graph_cycle_signature <- function(cycle_nodes) {
  nodes <- as.character(cycle_nodes %||% character())
  if (length(nodes) >= 2L && identical(nodes[[1]], nodes[[length(nodes)]])) {
    nodes <- nodes[-length(nodes)]
  }

  if (length(nodes) == 0L) {
    return("")
  }

  rotations <- vapply(
    seq_along(nodes),
    function(i) {
      prefix <- if (i <= 1L) character() else nodes[1:(i - 1L)]
      paste(c(nodes[i:length(nodes)], prefix), collapse = "|")
    },
    character(1)
  )

  sort(rotations)[[1]]
}

wf_graph_find_cycles <- function(nodes, edge_from, edge_to, max_cycles = 200L) {
  nodes <- unique(as.character(nodes %||% character()))
  edge_from <- as.character(edge_from %||% character())
  edge_to <- as.character(edge_to %||% character())

  if (length(edge_from) != length(edge_to)) {
    return(list(cycles = list(), truncated = FALSE))
  }

  if (length(edge_from) > 0L) {
    nodes <- unique(c(nodes, edge_from, edge_to))
  }

  if (length(nodes) == 0L) {
    return(list(cycles = list(), truncated = FALSE))
  }

  adj <- setNames(vector("list", length(nodes)), nodes)

  if (length(edge_from) > 0L) {
    for (i in seq_along(edge_from)) {
      from <- edge_from[[i]]
      to <- edge_to[[i]]
      if (!(from %in% names(adj)) || !(to %in% names(adj))) {
        next
      }
      if (!(to %in% adj[[from]])) {
        adj[[from]] <- c(adj[[from]], to)
      }
    }
  }

  visited <- setNames(rep(FALSE, length(nodes)), nodes)
  in_stack <- setNames(rep(FALSE, length(nodes)), nodes)
  stack <- character()
  cycles <- list()
  seen_signatures <- character()
  truncated <- FALSE

  add_cycle <- function(path) {
    signature <- wf_graph_cycle_signature(path)
    if (!nzchar(signature) || signature %in% seen_signatures) {
      return(invisible(NULL))
    }

    seen_signatures <<- c(seen_signatures, signature)

    refs <- lapply(
      strsplit(signature, "|", fixed = TRUE)[[1]],
      wf_graph_key_to_ref
    )

    cycles[[length(cycles) + 1L]] <<- list(
      path = refs,
      size = as.integer(length(refs))
    )

    if (length(cycles) >= max_cycles) {
      truncated <<- TRUE
    }

    invisible(NULL)
  }

  dfs <- function(node) {
    if (truncated) {
      return(invisible(NULL))
    }

    visited[[node]] <<- TRUE
    in_stack[[node]] <<- TRUE
    stack <<- c(stack, node)

    next_nodes <- sort(unique(as.character(adj[[node]] %||% character())))

    for (next_node in next_nodes) {
      if (truncated) {
        break
      }

      if (!(next_node %in% names(visited))) {
        next
      }

      if (!isTRUE(visited[[next_node]])) {
        dfs(next_node)
        next
      }

      if (isTRUE(in_stack[[next_node]])) {
        idx <- match(next_node, stack)
        if (!is.na(idx)) {
          add_cycle(c(stack[idx:length(stack)], next_node))
        }
      }
    }

    if (length(stack) > 0L) {
      stack <<- stack[-length(stack)]
    }
    in_stack[[node]] <<- FALSE

    invisible(NULL)
  }

  for (node in sort(nodes)) {
    if (truncated) {
      break
    }

    if (!isTRUE(visited[[node]])) {
      dfs(node)
    }
  }

  list(cycles = cycles, truncated = truncated)
}

wf_graph_toposort <- function(nodes, edge_from, edge_to) {
  node_keys <- unique(as.character(nodes %||% character()))
  edge_from <- as.character(edge_from %||% character())
  edge_to <- as.character(edge_to %||% character())

  if (length(node_keys) == 0L) {
    return(list(order = character(), has_cycle = FALSE))
  }

  indegree <- setNames(rep(0L, length(node_keys)), node_keys)
  adj <- setNames(vector("list", length(node_keys)), node_keys)

  if (length(edge_from) == length(edge_to) && length(edge_from) > 0L) {
    for (i in seq_along(edge_from)) {
      from <- edge_from[[i]]
      to <- edge_to[[i]]

      if (!(from %in% node_keys) || !(to %in% node_keys)) {
        next
      }

      if (!(to %in% adj[[from]])) {
        adj[[from]] <- c(adj[[from]], to)
        indegree[[to]] <- as.integer(indegree[[to]] + 1L)
      }
    }
  }

  queue <- sort(names(indegree)[indegree == 0L])
  out <- character()

  while (length(queue) > 0L) {
    node <- queue[[1]]
    queue <- queue[-1]

    out <- c(out, node)

    for (next_node in sort(unique(as.character(adj[[node]] %||% character())))) {
      indegree[[next_node]] <- as.integer(indegree[[next_node]] - 1L)
      if (indegree[[next_node]] == 0L) {
        queue <- sort(unique(c(queue, next_node)))
      }
    }
  }

  list(order = out, has_cycle = length(out) != length(node_keys))
}

wf_automation_execution_plan <- function(res, scope, automation_id, required_only = TRUE, cycle_limit = 100L) {
  id <- wf_id(automation_id)
  if (is.null(id)) {
    res$status <- 400
    return(list(error = "Automation id is required"))
  }

  root <- wf_find_object("automation", id, scope)
  if (is.null(root)) {
    res$status <- 404
    return(list(error = "Automation not found"))
  }

  dep_rows <- wf_graph_dependency_rows(
    scope = scope,
    source_type = "automation",
    target_type = "automation",
    required_only = required_only
  )

  dep_rows$source_id <- as.character(dep_rows$source_id %||% character())
  dep_rows$target_id <- as.character(dep_rows$target_id %||% character())

  to_visit <- c(id)
  visited <- character()

  while (length(to_visit) > 0L) {
    current <- to_visit[[1]]
    to_visit <- to_visit[-1]

    if (current %in% visited) {
      next
    }

    visited <- c(visited, current)

    direct_targets <- unique(dep_rows$target_id[dep_rows$source_id == current])
    direct_targets <- direct_targets[nzchar(direct_targets)]

    if (length(direct_targets) > 0L) {
      to_visit <- c(to_visit, direct_targets)
    }
  }

  plan_ids <- unique(visited)
  if (!(id %in% plan_ids)) {
    plan_ids <- c(id, plan_ids)
  }

  sub_rows <- dep_rows[
    dep_rows$source_id %in% plan_ids & dep_rows$target_id %in% plan_ids,
    ,
    drop = FALSE
  ]

  cycle_scan <- wf_graph_find_cycles(
    nodes = paste0("automation:", plan_ids),
    edge_from = paste0("automation:", as.character(sub_rows$source_id %||% character())),
    edge_to = paste0("automation:", as.character(sub_rows$target_id %||% character())),
    max_cycles = cycle_limit
  )

  if (length(cycle_scan$cycles) > 0L) {
    res$status <- 409
    return(
      list(
        error = "Automation dependencies contain cycle(s)",
        workspace = scope$workspace,
        project = scope$project,
        env = scope$env,
        automation_id = id,
        required_only = isTRUE(required_only),
        cycle_count = as.integer(length(cycle_scan$cycles)),
        cycles = cycle_scan$cycles,
        cycles_truncated = isTRUE(cycle_scan$truncated)
      )
    )
  }

  # Reverse dependency edges so topological order emits dependencies first.
  sort_result <- wf_graph_toposort(
    nodes = paste0("automation:", plan_ids),
    edge_from = paste0("automation:", as.character(sub_rows$target_id %||% character())),
    edge_to = paste0("automation:", as.character(sub_rows$source_id %||% character()))
  )

  if (isTRUE(sort_result$has_cycle)) {
    res$status <- 409
    return(list(error = "Could not compute execution order due to cycle"))
  }

  execution_order <- vapply(
    sort_result$order,
    function(key) wf_graph_key_to_ref(key)$object_id,
    character(1)
  )

  list(
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    automation_id = id,
    required_only = isTRUE(required_only),
    node_count = as.integer(length(plan_ids)),
    edge_count = as.integer(nrow(sub_rows)),
    execution_order = execution_order,
    dependencies = if (nrow(sub_rows) == 0L) list() else lapply(
      seq_len(nrow(sub_rows)),
      function(i) {
        list(
          source_id = as.character(sub_rows$source_id[[i]]),
          target_id = as.character(sub_rows$target_id[[i]]),
          required = isTRUE(sub_rows$required[[i]] %||% TRUE)
        )
      }
    )
  )
}

#* Validate dependency graph for cycles in scope.
#* @tag Workflows
#* @param required_only Include only required dependency edges (`true`/`false`).
#* @param limit:int Maximum cycles returned (1..1000).
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/validate/dependency-cycles
function(res, workspace = "personal", project = "default", env = "dev", required_only = TRUE, limit = 100) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  scope <- wf_scope(workspace, project, env)
  required_only <- wf_bool(required_only, default = TRUE)
  limit <- wf_clamp_int(limit, default = 100L, min_value = 1L, max_value = 1000L)

  dep_rows <- wf_graph_dependency_rows(scope = scope, required_only = required_only)

  scan <- wf_graph_find_cycles(
    nodes = unique(c(
      wf_graph_key(as.character(dep_rows$source_type %||% character()), as.character(dep_rows$source_id %||% character())),
      wf_graph_key(as.character(dep_rows$target_type %||% character()), as.character(dep_rows$target_id %||% character()))
    )),
    edge_from = wf_graph_key(as.character(dep_rows$source_type %||% character()), as.character(dep_rows$source_id %||% character())),
    edge_to = wf_graph_key(as.character(dep_rows$target_type %||% character()), as.character(dep_rows$target_id %||% character())),
    max_cycles = limit
  )

  list(
    valid = length(scan$cycles) == 0L,
    workspace = scope$workspace,
    project = scope$project,
    env = scope$env,
    required_only = required_only,
    edge_count = as.integer(nrow(dep_rows)),
    cycle_count = as.integer(length(scan$cycles)),
    cycles_truncated = isTRUE(scan$truncated),
    cycles = scan$cycles
  )
}

#* Compute dependency-first execution order for one automation.
#* @tag Workflows
#* @param required_only Include only required automation->automation edges (`true`/`false`).
#* @param cycle_limit:int Maximum cycles returned when a cycle exists (1..1000).
#* @serializer json list(auto_unbox = TRUE)
#* @get /v1/automations/<automation_id>/execution-plan
function(res, automation_id, workspace = "personal", project = "default", env = "dev", required_only = TRUE, cycle_limit = 100) {
  missing <- wf_ensure_tables(res)
  if (!is.null(missing)) {
    return(missing)
  }

  scope <- wf_scope(workspace, project, env)

  wf_automation_execution_plan(
    res = res,
    scope = scope,
    automation_id = automation_id,
    required_only = wf_bool(required_only, default = TRUE),
    cycle_limit = wf_clamp_int(cycle_limit, default = 100L, min_value = 1L, max_value = 1000L)
  )
}
