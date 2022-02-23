#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbListTables
#' @usage NULL
dbListTables__duckdb_connection <- function(conn, ...) {
  dbGetQuery(
    conn,
    SQL(
      "SELECT name FROM sqlite_master WHERE type='table' OR type='view' ORDER BY name"
    )
  )[[1]]
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbListTables", "duckdb_connection", dbListTables__duckdb_connection)
