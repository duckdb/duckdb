#' @rdname duckdb_result-class
#' @inheritParams DBI::dbIsValid
#' @usage NULL
dbIsValid__duckdb_result <- function(dbObj, ...) {
  return(dbObj@env$open)
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbIsValid", "duckdb_result", dbIsValid__duckdb_result)
