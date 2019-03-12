#' @include Driver.R
NULL

duckdb_connection <- function(duckdb_driver) {
  # TODO: Add arguments
  new("duckdb_connection", conn_ref=.Call(duckdb_connect_R, duckdb_driver@database_ref), driver=duckdb_driver)
}

#' @rdname DBI
#' @export
setClass(
  "duckdb_connection",
  contains = "DBIConnection",
  slots = list(conn_ref="externalptr", driver="duckdb_driver")
)

#' @rdname DBI
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "duckdb_connection",
  function(object) {
    cat("<duckdb_connection>\n")
  })

#' @rdname DBI
#' @inheritParams DBI::dbIsValid
#' @export
setMethod(
  "dbIsValid", "duckdb_connection",
  function(dbObj, ...) {
    valid <- FALSE
  tryCatch ({
    dbExecute(dbObj, SQL("SELECT 1"))
    valid <- TRUE
      }, error=function(c){})
  valid
  })

#' @rdname DBI
#' @inheritParams DBI::dbDisconnect
#' @export
setMethod(
  "dbDisconnect", "duckdb_connection",
  function(conn, ...) {
    if (!dbIsValid(conn)) {
      warning("Connection already closed.", call. = FALSE)
    }
   .Call(duckdb_disconnect_R, conn@conn_ref)

    invisible(TRUE)
  })

#' @rdname DBI
#' @inheritParams DBI::dbSendQuery
#' @export
setMethod(
  "dbSendQuery", c("duckdb_connection", "character"),
  function(conn, statement, ...) {
    resultset <- .Call(duckdb_query_R, conn@conn_ref, statement)
    attr(resultset, "row.names") <- c(NA_integer_, as.integer(-1 * length(resultset[[1]])))
    class(resultset) <- "data.frame"
    duckdb_result(connection = conn, statement = statement, has_resultset=TRUE, resultset=resultset)
  })

#' @rdname DBI
#' @inheritParams DBI::dbSendStatement
#' @export
setMethod(
  "dbSendStatement", c("duckdb_connection", "character"),
  function(conn, statement, ...) {
    resultset <- .Call(duckdb_query_R, conn@conn_ref, statement)
    duckdb_result(connection = conn, statement = statement, has_resultset=FALSE, rows_affected=resultset[[1]][1])
  })

#' @rdname DBI
#' @inheritParams DBI::dbDataType
#' @export
setMethod(
  "dbDataType", "duckdb_connection",
  function(dbObj, obj, ...) {
    dbDataType(dbObj@driver, obj, ...)
  })

#' @rdname DBI
#' @inheritParams DBI::dbWriteTable
#' @param overwrite Allow overwriting the destination table. Cannot be
#'   `TRUE` if `append` is also `TRUE`.
#' @param append Allow appending to the destination table. Cannot be
#'   `TRUE` if `overwrite` is also `TRUE`.
#' @export
setMethod(
  "dbWriteTable", c("duckdb_connection", "character", "data.frame"),
  function(conn, name, value, overwrite = FALSE, append = FALSE, ...) {
    testthat::skip("Not yet implemented: dbWriteTable(Connection, character, data.frame)")
  })

#' @rdname DBI
#' @inheritParams DBI::dbListTables
#' @export
setMethod(
  "dbListTables", "duckdb_connection",
  function(conn, ...) {
    dbGetQuery(conn, SQL("SELECT name FROM sqlite_master() WHERE type='table' ORDER BY name"))[[1]]
  })

#' @rdname DBI
#' @inheritParams DBI::dbExistsTable
#' @export
setMethod(
  "dbExistsTable", c("duckdb_connection", "character"),
  function(conn, name, ...) {
    sql_name <- dbQuoteString(conn, x = name, ...)
    query <- sqlInterpolate(conn, "SELECT COUNT(*) = 1 FROM sqlite_master() WHERE type='table' AND name=?", sql_name)
    dbGetQuery(conn, query)[[1]]
  })

#' @rdname DBI
#' @inheritParams DBI::dbListFields
#' @export
setMethod(
  "dbListFields", c("duckdb_connection", "character"),
  function(conn, name, ...) {
    sql_name <- dbQuoteString(conn, x = name, ...)
    query <- sqlInterpolate(conn, "SELECT name FROM pragma_table_info(?) ORDER BY cid", sql_name)
    dbGetQuery(conn, query)[[1]]
  })

#' @rdname DBI
#' @inheritParams DBI::dbRemoveTable
#' @export
setMethod(
  "dbRemoveTable", c("duckdb_connection", "character"),
  function(conn, name, ...) {
    sql_name <- dbQuoteIdentifier(conn, x = name, ...)
    query <- sqlInterpolate(conn, "DROP TABLE ?", sql_name)
    dbExecute(conn, query)
    invisible(TRUE)
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "duckdb_connection",
  function(dbObj, ...) {
    list(dbname=dbObj@driver@dbdir, db.version=NA, username=NA, host=NA, port=NA)
  })

#' @rdname DBI
#' @inheritParams DBI::dbBegin
#' @export
setMethod(
  "dbBegin", "duckdb_connection",
  function(conn, ...) {
   dbExecute(conn, SQL("BEGIN TRANSACTION"))
   invisible(TRUE)
  })

#' @rdname DBI
#' @inheritParams DBI::dbCommit
#' @export
setMethod(
  "dbCommit", "duckdb_connection",
  function(conn, ...) {
    dbExecute(conn, SQL("COMMIT"))
    invisible(TRUE)
  })

#' @rdname DBI
#' @inheritParams DBI::dbRollback
#' @export
setMethod(
  "dbRollback", "duckdb_connection",
  function(conn, ...) {
    dbExecute(conn, SQL("ROLLBACK"))
    invisible(TRUE)
  })
