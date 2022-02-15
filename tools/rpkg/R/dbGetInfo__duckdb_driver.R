#' @rdname duckdb_driver-class
#' @inheritParams DBI::dbGetInfo
#' @usage NULL
dbGetInfo__duckdb_driver <- function(dbObj, ...) {
  con <- dbConnect(dbObj)
  version <- dbGetQuery(con, "select library_version from pragma_version()")[[1]][[1]]
  dbDisconnect(con)
  list(driver.version = version, client.version = version, dbname = dbObj@dbdir)
}

#' @rdname duckdb_driver-class
#' @export
setMethod("dbGetInfo", "duckdb_driver", dbGetInfo__duckdb_driver)
