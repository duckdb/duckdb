#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbDataType
#' @usage NULL
dbDataType__duckdb_connection <- function(dbObj, obj, ...) {
  dbDataType(dbObj@driver, obj, ...)
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbDataType", "duckdb_connection", dbDataType__duckdb_connection)
