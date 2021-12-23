#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbGetInfo
#' @usage NULL
dbGetInfo__duckdb_connection <- function(dbObj, ...) {
  info <- dbGetInfo(dbObj@driver)
  list(
    dbname = info$dbname,
    db.version = info$driver.version,
    username = NA,
    host = NA,
    port = NA
  )
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbGetInfo", "duckdb_connection", dbGetInfo__duckdb_connection)
