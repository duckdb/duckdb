#' @description
#' `dbDisconnect()` closes a DuckDB database connection, optionally shutting down
#' the associated instance.
#'
#' @param conn A `duckdb_connection` object
#' @param shutdown
#'   Set to `TRUE` to shut down the DuckDB database instance that this connection refers to,
#'   if no other connections are using it.
#'   The default is to use the `auto_shutdown` argument
#'   that has been passed with `dbConnect()`.
#' @rdname duckdb
#' @usage NULL
dbDisconnect__duckdb_connection <- function(conn, ..., shutdown = NULL) {
  if (!dbIsValid(conn)) {
    warning("Connection already closed.", call. = FALSE)
    invisible(FALSE)
  }
  rapi_disconnect(conn@conn_ref)
  if (is.null(shutdown)) {
    shutdown <- conn@auto_shutdown
  }
  if (isTRUE(shutdown)) {
    rapi_shutdown(conn@driver@database_ref)
  }
  rs_on_connection_closed(conn)
  invisible(TRUE)
}

#' @rdname duckdb
#' @export
setMethod("dbDisconnect", "duckdb_connection", dbDisconnect__duckdb_connection)
