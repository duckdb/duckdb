#' @description
#' `dbDisconnect()` closes a DuckDB database connection, optionally shutting down
#' the associated instance.
#'
#' @param conn A `duckdb_connection` object
#' @param shutdown Set to `TRUE` to shut down the DuckDB database instance that this connection refers to.
#' @rdname duckdb
#' @usage NULL
dbDisconnect__duckdb_connection <- function(conn, ..., shutdown = FALSE) {
  if (!dbIsValid(conn)) {
    warning("Connection already closed.", call. = FALSE)
    invisible(FALSE)
  }
  rapi_disconnect(conn@conn_ref)
  if (shutdown) {
    duckdb_shutdown(conn@driver)
  }
  rs_on_connection_closed(conn)
  invisible(TRUE)
}

#' @rdname duckdb
#' @export
setMethod("dbDisconnect", "duckdb_connection", dbDisconnect__duckdb_connection)
