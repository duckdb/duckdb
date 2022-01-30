#' @export
expr_constant <- function(val) {
    res <- list(ref=.Call(`_duckdb_expr_constant_R`, val))
    class(res) <- c("duckdb_expr")
    res
}

#' @export
expr_reference <- function(ref) {
    res <- list(ref=.Call(`_duckdb_expr_reference_R`, ref))
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
rel_df <- function(con, df) {
    res <- list(ref=.Call(`_duckdb_rel_df_R`,con@conn_ref, as.data.frame(df)))
    class(res) <- c("duckdb_relation")
    res
}

rel_filter <- function(rel, expr) {
    res <- list(df=.Call(`_duckdb_rel_filter_R`, rel$ref, expr$ref))
    class(res) <- c("duckdb_relation")
    res
}


