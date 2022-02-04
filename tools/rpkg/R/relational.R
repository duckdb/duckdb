# these wrappers are a bit annoying, maybe there's a way around. Kirill?

#' @export
expr_reference <- expr_reference_cpp

#' @export
expr_constant <- expr_constant_cpp

#' @export
expr_function <- expr_function_cpp

#' @export
rel_project <- rel_project_cpp

#' @export
rel_filter <- rel_filter_cpp

#' @export
rel_aggregate <- rel_aggregate_cpp

#' @export
rel_order <- rel_order_cpp

#' @export
print.duckdb_expr <- function(expr) {
    message("DuckDB Expression: ", expr_tostring(expr))
    invisible(NULL)
}

#' @export
rel_from_df <- function(con, df) {
    rel_from_df_cpp(con@conn_ref, as.data.frame(df))
}

#' @export
as.data.frame.duckdb_relation <- function(rel) {
    rel_to_df_cpp(rel)
}

#' @export
print.duckdb_relation <- function(rel) {
    message("DuckDB Relation: ", rel_tostring(rel))
}

#' @export
rel_explain <- function(rel) {
    cat(rel_explain_cpp(rel)[[2]][[1]])
    invisible(NULL)
}

