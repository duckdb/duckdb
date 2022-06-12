# Registered in .onLoad()
unnest.tbl_duckdb_connection <- function(data,
                                         cols,
                                         ...,
                                         keep_empty = FALSE,
                                         ptype = NULL,
                                         names_sep = NULL,
                                         names_repair = "check_unique") {

  if (keep_empty) {
    stop("`keep_empty` unsupported.")
  }
  if (!is.null(ptype)) {
    stop("`ptype` unsupported.")
  }

  # Side effect: check that tidyr is loaded, brings in rlang, vctrs, tidyselect
  # and dplyr
  pkg_method("unnest", "tidyr")
  rlang::check_required(cols)

  named_names <- rlang::set_names(colnames(data))

  cols_q <- rlang::enquo(cols)
  select_cols <- tidyselect::eval_select(cols_q, named_names)

  plan <- lapply(rlang::set_names(names(select_cols)), function(.x) {
    call("UNNEST", as.name(.x))
  })

  unnested <- dplyr::mutate(data, !!!plan)

  unpack.tbl_duckdb_connection(
    unnested, !!cols_q,
    names_sep = names_sep,
    names_repair = names_repair
  )
}

# Registered in .onLoad()
unpack.tbl_duckdb_connection <- function(data, cols, ..., names_sep = NULL, names_repair = "check_unique") {
  if (!is.null(names_sep)) {
    stop("`names_sep` unsupported.")
  }

  # Side effect: check that tidyr is loaded, brings in rlang, vctrs, tidyselect
  # and dplyr
  pkg_method("unpack", "tidyr")
  rlang::check_required(cols)

  named_names <- rlang::set_names(colnames(data))

  cols <- tidyselect::eval_select(rlang::enquo(cols), named_names)

  ptype <- dplyr::collect(utils::head(dplyr::select(data, !!!cols), 0))
  ptype <- ptype[vapply(ptype, is.data.frame, logical(1))]

  plan <- lapply(named_names, function(.x) list(as.name(.x)))
  plan[names(ptype)] <- mapply(ptype, names(ptype), FUN = function (.x, .y) {
    out <- lapply(rlang::set_names(names(.x)), function(...) {
      list(call("STRUCT_EXTRACT", as.name(.y), ..1))
    })
    list(vctrs::new_data_frame(out, n = 1L))
  })

  names(plan)[names(plan) %in% names(ptype)] <- ""

  # Logic from tidyr
  df_plan <- vctrs::df_list(!!!plan, .size = 1, .name_repair = "minimal")
  names(df_plan) <- vctrs::vec_as_names(
    names = names(df_plan),
    repair = names_repair,
    repair_arg = "names_repair"
  )

  unpacked_plan <- lapply(df_plan, function(.x) .x[[1]])

  dplyr::transmute(data, !!!unpacked_plan)
}
