#' Override the finalize call of the duckdb sum function so that
#' if the result is not set, set it to 0
#' @param conn A DuckDB connection, created by `dbConnect()`.
#' @return a boolean if the overwrite was successful or not
#' @noRd
#' @examples
#' conn <- dbConnect(duckdb::duckdb())
#' duckdb_set_sum_default_to_zero(conn)
duckdb_set_sum_default_to_zero <- function(conn) {
  rapi_set_sum_default_to_zero(conn@conn_ref)
}
