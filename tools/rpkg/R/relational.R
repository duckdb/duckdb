#' @export
expr_scalar <- function(val) {
    res <- list(type="expr_constant", ref=.Call(`_duckdb_expr_constant_R`, val))
    class(res) <- c("duckdb_expr", "duckdb_expr_constant")
    res
}

#' @export
expr_ref <- function(ref) {
    res <- list(type="expr_ref", ref=.Call(`_duckdb_expr_ref_R`, ref))
    class(res) <- c("duckdb_expr", "duckdb_expr_ref")
    res
}

#' @export
rel_from_df <- function(df) {
    res <- list(df=as.data.frame(ref))
    class(res) <- c("duckdb_rel")
    res
}
