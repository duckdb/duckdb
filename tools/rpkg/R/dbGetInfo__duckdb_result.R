#' @rdname duckdb_result-class
#' @inheritParams DBI::dbGetInfo
#' @usage NULL
dbGetInfo__duckdb_result <- function(dbObj, ...) {
  # Optional
  getMethod("dbGetInfo", "DBIResult", asNamespace("DBI"))(dbObj, ...)
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbGetInfo", "duckdb_result", dbGetInfo__duckdb_result)
