#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbRollback
#' @usage NULL
dbRollback__duckdb_connection <- function(conn, ...) {
  dbExecute(conn, SQL("ROLLBACK"))
  invisible(TRUE)
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbRollback", "duckdb_connection", dbRollback__duckdb_connection)
