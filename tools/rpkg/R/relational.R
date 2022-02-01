#' @export
expr_constant <- function(val) {
    structure(.Call(`_duckdb_expr_constant_R`, val), class="duckdb_expr")
}

#' @export
expr_reference <- function(ref) {
    structure(.Call(`_duckdb_expr_reference_R`, as.character(ref)), class="duckdb_expr")
}

#' @export
expr_function <- function(name, args) {
    structure(.Call(`_duckdb_expr_function_R`, name, args), class="duckdb_expr")
}

#' @export
expr_tostring <- function(expr) {
   .Call(`_duckdb_expr_tostring_R`, expr)
}

#' @export
print.duckdb_expr <- function(expr) {
    message("DuckDB Expression: ", expr_tostring(expr))
    invisible(NULL)
}

#' @export
rel_from_df <- function(con, df) {
    structure(.Call(`_duckdb_rel_from_df_R`, con@conn_ref, as.data.frame(df)), class="duckdb_relation")
}

#' @export
rel_filter <- function(rel, expr) {
    structure(.Call(`_duckdb_rel_filter_R`, rel, expr), class="duckdb_relation")
}

#' @export
rel_project <- function(rel, exprs) {
    structure(.Call(`_duckdb_rel_project_R`, rel, exprs), class="duckdb_relation")
}

#' @export
rel_aggregate <- function(rel, groups, aggregates) {
    structure(.Call(`_duckdb_rel_aggregate_R`, rel, groups, aggregates), class="duckdb_relation")
}

#' @export
rel_order <- function(rel, orders) {
    structure(.Call(`_duckdb_rel_order_R`, rel, orders), class="duckdb_relation")
}


#' @export
as.data.frame.duckdb_relation <- function(rel) {
    x <- .Call(`_duckdb_rel_to_df_R`, rel)
    attr(x, "row.names") <- c(NA_integer_, -length(x[[1]]))
    class(x) <- "data.frame"
    x
}

#' @export
print.duckdb_relation <- function(rel) {
    print(as.data.frame(rel))
}


