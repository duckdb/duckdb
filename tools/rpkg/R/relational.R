#' @export
expr_constant <- function(val) {
    res <- list(ref=.Call(`_duckdb_expr_constant_R`, val))
    class(res) <- c("duckdb_expr")
    res
}

#' @export
expr_reference <- function(ref) {
    res <- list(ref=.Call(`_duckdb_expr_reference_R`, as.character(ref)))
    class(res) <- c("duckdb_expr")
    res
}

#' @export
expr_function <- function(name, args) {
    args_extptrs <- lapply(args, function(arg) {
        stopifnot(inherits(arg, "duckdb_expr"))
        arg$ref
    })

    res <- list(ref=.Call(`_duckdb_expr_function_R`, name, args_extptrs))
    class(res) <- c("duckdb_expr")
    res
}

#' @export
expr_tostring <- function(expr) {
   stopifnot(inherits(expr, "duckdb_expr"))
   .Call(`_duckdb_expr_tostring_R`, expr$ref)
}

#' @export
print.duckdb_expr <- function(expr) {
    message("DuckDB Expression: ", expr_tostring(expr))
    invisible(NULL)
}

#' @export
rel_from_df <- function(con, df) {
    stopifnot(inherits(df, "data.frame"))
    res <- list(ref=.Call(`_duckdb_rel_from_df_R`,con@conn_ref, as.data.frame(df)))
    class(res) <- c("duckdb_relation")
    res
}

#' @export
rel_filter <- function(rel, expr) {
    stopifnot(inherits(rel, "duckdb_relation"))
    stopifnot(inherits(expr, "duckdb_expr"))
    res <- list(ref=.Call(`_duckdb_rel_filter_R`, rel$ref, expr$ref))
    class(res) <- c("duckdb_relation")
    res
}

#' @export
as.data.frame.duckdb_relation <- function(rel) {
    stopifnot(inherits(rel, "duckdb_relation"))
    x <- .Call(`_duckdb_rel_to_df_R`, rel$ref)
    attr(x, "row.names") <- c(NA_integer_, -length(x[[1]]))
    class(x) <- "data.frame"
    x
}

#' @export
print.duckdb_relation <- function(rel) {
    print(as.data.frame(rel))
}


