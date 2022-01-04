#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbIsValid
#' @usage NULL
dbIsValid__duckdb_connection <- function(dbObj, ...) {
  valid <- FALSE
  tryCatch(
    {
      dbGetQuery(dbObj, SQL("SELECT 1"))
      valid <- TRUE
    },
    error = function(c) {
    }
  )
  valid
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbIsValid", "duckdb_connection", dbIsValid__duckdb_connection)
