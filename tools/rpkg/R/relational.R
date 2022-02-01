#' @export
print.duckdb_expr <- function(expr) {
    message("DuckDB Expression: ", expr_tostring(expr))
    invisible(NULL)
}

# we have a wrapper to simplify calling as.data.frame() on argument
#' @export
rel_from_df <- function(con, df) {
    .Call(`_duckdb_rel_from_df_R`, con@conn_ref, as.data.frame(df))
}

#' @export
as.data.frame.duckdb_relation <- function(rel) {
    .Call(`_duckdb_rel_to_df`, rel)
}

#' @export
print.duckdb_relation <- function(rel) {
    message("DuckDB Relation: ", rel_tostring(rel))
}

#' @export
rel_explain <- function(rel) {
    cat(.Call(`_duckdb_rel_explain_R`,rel)[[2]][[1]])
    invisible(NULL)
}
