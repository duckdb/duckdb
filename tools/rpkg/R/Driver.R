#' @include duckdb.R
NULL

#' DBI methods
#'
#' Implementations of pure virtual functions defined in the `DBI` package.
#' @name DBI
NULL

#' DuckDB driver
#'
#' TBD.
#'
#' @export
#' @import methods DBI
#' @examples
#' \dontrun{
#' #' library(DBI)
#' duckdb::duckdb()
#' }
duckdb <- function() {
  new("duckdb_driver")
}

#' @rdname DBI
#' @export
setClass("duckdb_driver", contains = "DBIDriver")

extptr_str <- function(e, n=5) {
  x <- .Call(duckdb_ptr_to_str, e)
  substr(x, nchar(x)-n+1, nchar(x))
}

#' @rdname DBI
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "duckdb_driver",
  function(object) {
    cat("<duckdb_driver>\n")
  })

#' @rdname DBI
#' @inheritParams DBI::dbConnect
#' @export
setMethod(
  "dbConnect", "duckdb_driver",
  function(drv, dbdir=":memory:", ..., debug=getOption("duckdb.debug", FALSE), dbname=NA, read_only=FALSE) {
    if (!missing(dbname)) {
      dbdir <- dbname
    }
    duckdb_connection(drv, dbdir=dbdir, debug=debug, read_only=read_only)
  }
)

#' @rdname DBI
#' @inheritParams DBI::dbDataType
#' @export
setMethod(
  "dbDataType", "duckdb_driver",
  function(dbObj, obj, ...) {

  if (is.null(obj)) stop("NULL parameter")
  if (is.data.frame(obj)) {
    return (vapply(obj, function(x) dbDataType(dbObj, x), FUN.VALUE = "character"))
  }
#  else if (int64 && inherits(obj, "integer64")) "BIGINT"
  else if (inherits(obj, "Date")) "DATE"
  else if (inherits(obj, "difftime")) "TIME"
  else if (is.logical(obj)) "BOOLEAN"
  else if (is.integer(obj)) "INTEGER"
  else if (is.numeric(obj)) "DOUBLE"
  else if (inherits(obj, "POSIXt")) "TIMESTAMP"
  else if (is.list(obj) && all(vapply(obj, typeof, FUN.VALUE = "character") == "raw" || is.na(obj))) "BLOB"
  else "STRING"

  })

#' @rdname DBI
#' @inheritParams DBI::dbIsValid
#' @export
setMethod(
  "dbIsValid", "duckdb_driver",
  function(dbObj, ...) {
    valid <- FALSE
    tryCatch ({
      con <- dbConnect(dbObj)
      dbExecute(con, SQL("SELECT 1"))
      dbDisconnect(con)
      valid <- TRUE
    }, error = function(c) {
    })
    valid
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "duckdb_driver",
  function(dbObj, ...) {
    list(driver.version=NA, client.version=NA)
  })


#' @export
duckdb_shutdown <- function(drv) {
  stop("FIXME")
  if (!is(drv, "duckdb_driver")) {
    stop("pass a duckdb_driver object")
  }
  if (!dbIsValid(drv)) {
    warning("invalid driver object, already closed?")
    invisible(FALSE)
  }
  .Call(duckdb_shutdown_R, drv@database_ref)
  invisible(TRUE)
}

is_installed <- function (pkg) {
    as.logical(requireNamespace(pkg, quietly = TRUE)) == TRUE
}


#' @export
src_duckdb <- function (path=":memory:", create = FALSE) {
    if (!is_installed("dbplyr")) {
      stop("Need package `dbplyr` installed.")
    }
    if (path != ":memory:" && !create && !file.exists(path)) {
        stop("`path` '",path,"' must already exist, unless `create` = TRUE")
    }
    con <- DBI::dbConnect(duckdb::duckdb(), path)
    dbplyr::src_dbi(con, auto_disconnect = TRUE)
}
