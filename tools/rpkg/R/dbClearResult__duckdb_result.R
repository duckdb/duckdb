#' @rdname duckdb_result-class
#' @inheritParams DBI::dbClearResult
#' @usage NULL
dbClearResult__duckdb_result <- function(res, ...) {
  if (res@env$open) {
    .Call(`_duckdb_release_R`, res@stmt_lst$ref)
    res@env$open <- FALSE
  } else {
    warning("Result was cleared already")
  }
  return(invisible(TRUE))
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbClearResult", "duckdb_result", dbClearResult__duckdb_result)
