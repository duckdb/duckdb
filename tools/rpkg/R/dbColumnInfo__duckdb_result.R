#' @rdname duckdb_result-class
#' @inheritParams DBI::dbColumnInfo
#' @usage NULL
dbColumnInfo__duckdb_result <- function(res, ...) {
  if (!res@env$open) {
    stop("result has already been cleared")
  }
  return(data.frame(name = res@stmt_lst$names, type = res@stmt_lst$rtypes, stringsAsFactors = FALSE))
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbColumnInfo", "duckdb_result", dbColumnInfo__duckdb_result)
