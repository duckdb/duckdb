#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbExistsTable
#' @usage NULL
dbExistsTable__duckdb_connection_character <- function(conn, name, ...) {
  if (!dbIsValid(conn)) {
    stop("Invalid connection")
  }
  if (length(name) != 1) {
    stop("Can only have a single name argument")
  }
  dbGetQuery(conn, "SELECT COUNT(*) > 0 AS found from information_schema.tables WHERE table_name=?", name)$found
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbExistsTable", c("duckdb_connection", "character"), dbExistsTable__duckdb_connection_character)
