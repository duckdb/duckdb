#' Run a SQL query
#'
#' `sql()` runs an arbitrary SQL query and returns a data.frame the query results
#'
#' @param sql A SQL string
#' @param conn An optional connection, defaults to built-in default
#' @return A data frame with the query result
#' @noRd
#' @examples
#' print(duckdb::sql("SELECT 42"))

sql <- function(sql, conn = default_connection()) {
  stopifnot(dbIsValid(conn))
  dbGetQuery(conn, sql)
}

default_duckdb_connection <- new.env(parent=emptyenv())

#' Get the default connection
#'
#' `default_connection()` returns a default, built-in connection
#'
#' @return A DuckDB connection object
#' @noRd
#' @examples
#' conn <- default_connection()
#' print(duckdb::sql("SELECT 42", conn=conn))
default_connection <- function() {
  if(!exists("con", default_duckdb_connection)) {
    con <- DBI::dbConnect(duckdb::duckdb())

    default_duckdb_connection$con <- con

    reg.finalizer(default_duckdb_connection, onexit = TRUE, function(e) {
      DBI::dbDisconnect(e$con, shutdown = TRUE)
    })
  }
  default_duckdb_connection$con
}
