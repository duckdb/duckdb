#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbRemoveTable
#' @usage NULL
dbRemoveTable__duckdb_connection_character <- function(conn, name, ..., fail_if_missing = TRUE) {
  sql <- paste0("DROP TABLE ", if (!fail_if_missing) "IF EXISTS ", "?")
  dbExecute(
    conn,
    sqlInterpolate(conn, sql, dbQuoteIdentifier(conn, name))
  )
  rs_on_connection_updated(conn, "Table removed")
  invisible(TRUE)
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbRemoveTable", c("duckdb_connection", "character"), dbRemoveTable__duckdb_connection_character)
