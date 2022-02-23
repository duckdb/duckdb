#' @rdname duckdb_result-class
#' @inheritParams methods::show
#' @usage NULL
show__duckdb_result <- function(object) {
  message(sprintf("<duckdb_result %s connection=%s statement='%s'>", extptr_str(object@stmt_lst$ref), extptr_str(object@connection@conn_ref), object@stmt_lst$str))
  invisible(NULL)
}

#' @rdname duckdb_result-class
#' @export
setMethod("show", "duckdb_result", show__duckdb_result)
