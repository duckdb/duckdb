#' @rdname duckdb_result-class
#' @inheritParams DBI::dbBind
#' @usage NULL
dbBind__duckdb_result <- function(res, params, ...) {
  if (!res@env$open) {
    stop("result has already been cleared")
  }
  res@env$rows_fetched <- 0
  res@env$resultset <- data.frame()

  params <- as.list(params)
  if (!is.null(names(params))) {
    stop("`params` must not be named")
  }

  params <- encode_values(params)

  out <- rapi_bind(res@stmt_lst$ref, params, res@arrow, res@connection@driver@bigint == "integer64")
  if (length(out) == 1) {
    out <- out[[1]]
  } else if (length(out) == 0) {
    out <- data.frame()
  } else {
    out <- do.call(rbind, lapply(out, list_to_df))
  }
  duckdb_post_execute(res, out)
  invisible(res)
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbBind", "duckdb_result", dbBind__duckdb_result)

list_to_df <- function(x) {
  if (is.data.frame(x)) {
    return(x)
  }
  attr(x, "row.names") <- c(NA_integer_, -NROW(x[[1]]))
  class(x) <- "data.frame"
  x
}
