#' @rdname duckdb_result-class
#' @inheritParams DBI::dbGetRowCount
#' @usage NULL
dbGetRowCount__duckdb_result <- function(res, ...) {
  if (!res@env$open) {
    stop("result has already been cleared")
  }
  return(res@env$rows_fetched)
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbGetRowCount", "duckdb_result", dbGetRowCount__duckdb_result)
