#' DuckDB driver class
#'
#' Implements \linkS4class{DBIDriver}.
#'
#' @aliases duckdb_driver
#' @keywords internal
#' @export
setClass("duckdb_driver", contains = "DBIDriver", slots = list(database_ref = "externalptr", dbdir = "character", read_only = "logical", bigint="character"))

#' DuckDB connection class
#'
#' Implements \linkS4class{DBIConnection}.
#'
#' @aliases duckdb_connection
#' @keywords internal
#' @export
setClass("duckdb_connection",
  contains = "DBIConnection",
  slots = list(
    conn_ref = "externalptr",
    driver = "duckdb_driver",
    debug = "logical",
    timezone_out = "character",
    tz_out_convert = "character",
    reserved_words = "character"
  )
)

duckdb_connection <- function(duckdb_driver, debug) {
  out <- new(
    "duckdb_connection",
    conn_ref = rapi_connect(duckdb_driver@database_ref),
    driver = duckdb_driver,
    debug = debug,
    timezone_out = "UTC",
    tz_out_convert = "with"
  )
  out@reserved_words <- get_reserved_words(out)
  out
}

duckdb_random_string <- function(x) {
  paste(sample(letters, 10, replace = TRUE), collapse = "")
}
