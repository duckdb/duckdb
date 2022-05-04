#' Query DuckDB using Substrait
#' TODO document this
#'
#' @param conn A DuckDB connection, created by `dbConnect()`.
#' @param query The Protobuf-encoded Substrait Query Plan. Qack!
#' @param arrow Whether the result should be in Arrow format
#' @return A DuckDB Query Result
#' @export
duckdb_prepare_substrait <- function(conn, query, arrow = FALSE) {
  stopifnot(dbIsValid(conn))
  stopifnot(is.raw(query))
  stmt_lst <- rapi_prepare_substrait(conn@conn_ref, query)
  duckdb_result(
      connection = conn,
      stmt_lst = stmt_lst,
      arrow = arrow
    )
}



#' Get the Substrait plan for a SQL query
#' TODO document this
#'
#' @param conn A DuckDB connection, created by `dbConnect()`.
#' @param query The query string in SQL
#' @return A raw vector containing the substrait protobuf blob
#' @export
duckdb_get_substrait <- function(conn, query) {
  stopifnot(dbIsValid(conn))
  stopifnot(is.character(query))
  rapi_get_substrait(conn@conn_ref, query)
}
