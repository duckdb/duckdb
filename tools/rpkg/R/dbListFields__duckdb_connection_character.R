#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbListFields
#' @usage NULL
dbListFields__duckdb_connection_character <- function(conn, name, ...) {
  names(dbGetQuery(
    conn,
    sqlInterpolate(
      conn,
      "SELECT * FROM ? WHERE FALSE",
      dbQuoteIdentifier(conn, name)
    )
  ))
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbListFields", c("duckdb_connection", "character"), dbListFields__duckdb_connection_character)
