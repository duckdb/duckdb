#' @rdname duckdb_connection-class
#' @inheritParams methods::show
#' @usage NULL
show__duckdb_connection <- function(object) {
  message(sprintf("<duckdb_connection %s driver=%s>", extptr_str(object@conn_ref), drv_to_string(object@driver)))
  invisible(NULL)
}

#' @rdname duckdb_connection-class
#' @export
setMethod("show", "duckdb_connection", show__duckdb_connection)
