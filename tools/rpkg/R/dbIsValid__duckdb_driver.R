#' @rdname duckdb_driver-class
#' @inheritParams DBI::dbIsValid
#' @importFrom DBI dbConnect
#' @usage NULL
dbIsValid__duckdb_driver <- function(dbObj, ...) {
  valid <- FALSE
  tryCatch(
    {
      con <- dbConnect(dbObj)
      dbExecute(con, SQL("SELECT 1"))
      dbDisconnect(con)
      valid <- TRUE
    },
    error = function(c) {
    }
  )
  valid
}

#' @rdname duckdb_driver-class
#' @export
setMethod("dbIsValid", "duckdb_driver", dbIsValid__duckdb_driver)
