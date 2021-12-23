#' @rdname duckdb_driver-class
#' @inheritParams methods::show
#' @usage NULL
show__duckdb_driver <- function(object) {
  message(drv_to_string(object))
  invisible(NULL)
}

#' @rdname duckdb_driver-class
#' @export
setMethod("show", "duckdb_driver", show__duckdb_driver)
