#' DuckDB EXPLAIN query tree
#' @rdname duckdb_explain-class
#' @export
duckdb_explain <- setClass("duckdb_explain", contains = "data.frame", slots = list(explain_key = "character", explain_value = "character"))

#' @rdname duckdb_explain-class
#' @usage NULL
#' @export
print.duckdb_explain <- function(x, ...) {
  if (!all(names(x) %in% c("explain_key", "explain_value"))) NextMethod("print")
  for (i in 1:nrow(x)) cat(paste0(x$explain_key[i], "\n", x$explain_value[i]))
  invisible(x)
}
