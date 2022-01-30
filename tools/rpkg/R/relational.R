#' @export
expr_constant <- function(val) {
    res <- list(type="expr_constant", ref=.Call(`_duckdb_expr_constant_R`, val))
    class(res) <- c("duckdb_expr_constant", "duckdb_expr")
    res
}

#' @export
expr_reference <- function(ref) {
    res <- list(type="expr_reference", ref=.Call(`_duckdb_expr_reference_R`, ref))
    class(res) <- c("duckdb_expr_reference", "duckdb_expr")
    res
}

#' @export
expr_function <- function(name, lhs, rhs) {
    stopifnot(inherits(lhs, "duckdb_expr"))
    stopifnot(inherits(rhs, "duckdb_expr"))

    res <- list(type="expr_function", ref=.Call(`_duckdb_expr_function_R`, name, lhs$ref, rhs$ref))
    class(res) <- c("duckdb_expr_function", "duckdb_expr")
    res
}

#' @export
expr_tostring <- function(expr) {
   stopifnot(inherits(expr, "duckdb_expr"))
   .Call(`_duckdb_expr_tostring_R`, expr$ref)
}


#' @export
rel_from_df <- function(df) {
    res <- list(df=as.data.frame(ref))
    class(res) <- c("duckdb_relation")
    res
}
