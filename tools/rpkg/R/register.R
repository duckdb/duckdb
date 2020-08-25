#' Register a R data.frame as a virtual table (view) in DuckDB without copying the data
#' @rdname duckdb_connection
#' @param conn A DuckDB connection, created by `dbConnect()`.
#' @param name The name for the virtual table that is registered
#' @param df A `data.frame` with the data for the virtual table
#' @export
duckdb_register <- function(conn, name, df) {
  stopifnot(dbIsValid(conn))
  .Call(duckdb_register_R, conn@conn_ref, as.character(name), as.data.frame(df))
  invisible(TRUE)
}

#' Unregister a virtual table referring to a data.frame
#' @rdname duckdb_connection
#' @param conn A DuckDB connection, created by `dbConnect()`.
#' @param name The name for the virtual table previously registered using `duckdb_register()`.
#' @export
duckdb_unregister <- function(conn, name) {
  stopifnot(dbIsValid(conn))
  .Call(duckdb_unregister_R, conn@conn_ref, as.character(name))
  invisible(TRUE)
}
