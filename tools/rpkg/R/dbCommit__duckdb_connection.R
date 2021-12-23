#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbCommit
#' @usage NULL
dbCommit__duckdb_connection <- function(conn, ...) {
  dbExecute(conn, SQL("COMMIT"))
  invisible(TRUE)
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbCommit", "duckdb_connection", dbCommit__duckdb_connection)
