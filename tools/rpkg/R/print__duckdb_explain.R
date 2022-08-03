#' DuckDB EXPLAIN query tree
#' @rdname duckdb_explain-class
#' @aliases duckdb_explain
#' @export
setClass("duckdb_explain", contains = "data.frame", slots = list(explain_key = "character", explain_value = "character"))

#' @rdname duckdb_explain-class
#' @usage NULL
#' @export
print.duckdb_explain <- function(x, ...) {
  if (!all(names(x) %in% c("explain_key", "explain_value"))) NextMethod("print")
  if (nrow(x) > 0 & all(!is.na(x))) cat(paste0(x$explain_key, "\n", x$explain_value, collapse = ""))
  invisible(x)
}
