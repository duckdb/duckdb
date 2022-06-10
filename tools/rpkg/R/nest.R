# Registered in .onLoad()
nest.tbl_duckdb_connection <- function(.data, ..., .names_sep = NULL) {
  # Import
  group_by <- pkg_method("group_by", "dplyr", "nest")
  summarise <- pkg_method("summarise", "dplyr", "nest", version = "1.0.0")

  # Build spec
  spec <- build_nest_pack_spec(.data, .names_sep, ...)

  # Plan
  groups <- lapply(unname(spec$unpacked), as.name)

  list_calls <- lapply(stats::setNames(nm = names(spec$packed)), function(.x) {
    call("LIST", as.name(.x))
  })

  # Apply plan
  packed_tbl <- pack_impl(.data, spec$unpacked, spec$packed)
  grouped_tbl <- group_by(packed_tbl, !!!groups)
  summarised_tbl <- summarise(grouped_tbl, !!!list_calls, .groups = "drop")
  summarised_tbl
}

# Registered in .onLoad()
pack.tbl_duckdb_connection <- function(.data, ..., .names_sep = NULL) {
  # Build spec
  spec <- build_nest_pack_spec(.data, .names_sep, ...)

  # Apply plan
  pack_impl(.data, spec$unpacked, spec$packed)
}

build_nest_pack_spec <- function(.data, .names_sep, ...) {
  if (!is.null(.names_sep)) {
    stop("`.names_sep` unsupported.")
  }

  enquos <- pkg_method("enquos", "rlang", "pack")
  names2 <- pkg_method("names2", "rlang", "pack")
  eval_select <- pkg_method("eval_select", "tidyselect", "pack")

  named_names <- stats::setNames(nm = colnames(.data))

  cols <- enquos(...)
  if (any(names2(cols) == "")) {
    stop("All elements of `...` must be named")
  }

  cols <- lapply(cols, function(.x) eval_select(.x, named_names))

  unpacked <- setdiff(named_names, unlist(lapply(cols, names)))

  packed <- lapply(cols, function(.x) named_names[.x])

  # if (!is.null(.names_sep)) {
  #   packed <- imap(packed, strip_names, names_sep = .names_sep)
  # }

  list(named_names = named_names, packed = packed, unpacked = unpacked)
}

pack_impl <- function(.data, unpacked, packed) {
  transmute <- pkg_method("transmute", "dplyr", "pack")

  stopifnot(identical(unname(unlist(lapply(packed, names))), unname(unlist(packed))))

  row_calls <- lapply(packed, function(.x) {
    fun_args <- lapply(c("ROW", unname(.x)), as.name)
    as.call(fun_args)
  })

  transmute(.data, !!!lapply(unpacked, as.name), !!!row_calls)
}

untibble <- function(x) {
  if (!is.data.frame(x)) {
    return(x)
  }
  class(x) <- "data.frame"
  dfs <- vapply(x, is.data.frame, logical(1))
  lists <- vapply(x[!dfs], is.list, logical(1))

  x[dfs] <- lapply(x[dfs], untibble)
  x[!dfs][lists] <- lapply(x[!dfs][lists], lapply, untibble)
  x
}
