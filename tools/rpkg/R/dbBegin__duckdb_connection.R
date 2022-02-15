#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbBegin
#' @usage NULL
dbBegin__duckdb_connection <- function(conn, ...) {
  dbExecute(conn, SQL("BEGIN TRANSACTION"))
  invisible(TRUE)
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbBegin", "duckdb_connection", dbBegin__duckdb_connection)
