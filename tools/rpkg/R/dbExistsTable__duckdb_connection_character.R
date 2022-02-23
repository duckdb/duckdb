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
  exists <- FALSE
  tryCatch(
    {
      dbGetQuery(
        conn,
        sqlInterpolate(
          conn,
          "SELECT * FROM ? WHERE FALSE",
          dbQuoteIdentifier(conn, name)
        )
      )
      exists <- TRUE
    },
    error = function(c) {
    }
  )
  exists
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbExistsTable", c("duckdb_connection", "character"), dbExistsTable__duckdb_connection_character)
