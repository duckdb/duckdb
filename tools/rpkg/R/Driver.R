DBDIR_MEMORY <- ":memory:"

check_flag <- function(x) {
  if (is.null(x) || length(x) != 1 || is.na(x) || !is.logical(x)) {
    stop("flags need to be scalar logicals")
  }
}

extptr_str <- function(e, n = 5) {
  x <- .Call(duckdb_ptr_to_str, e)
  substr(x, nchar(x) - n + 1, nchar(x))
}

drv_to_string <- function(drv) {
  if (!is(drv, "duckdb_driver")) {
    stop("pass a duckdb_driver object")
  }
  sprintf("<duckdb_driver %s dbdir='%s' read_only=%s>", extptr_str(drv@database_ref), drv@dbdir, drv@read_only)
}

#' @rdname duckdb_driver-class
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "duckdb_driver",
  function(object) {
    cat(drv_to_string(object))
    cat("\n")
  }
)

#' Connect to a DuckDB database instance
#'
#' `dbConnect()` connects to a database instance.
#'
#' @param drv Object returned by `duckdb()`
#' @param dbdir Location for database files. Should be a path to an existing
#'   directory in the file system. With the default, all
#'   data is kept in RAM
#' @param ... Ignored
#' @param debug Print additional debug information such as queries
#' @param read_only Set to `TRUE` for read-only operation
#'
#' @return `dbConnect()` returns an object of class
#'   \linkS4class{duckdb_connection}.
#'
#' @rdname duckdb
#' @export
#' @examples
#' drv <- duckdb()
#' con <- dbConnect(drv)
#'
#' dbGetQuery(con, "SELECT 'Hello, world!'")
#'
#' dbDisconnect(con)
#' duckdb_shutdown(drv)
#'
#' # Shorter:
#' con <- dbConnect(duckdb())
#' dbGetQuery(con, "SELECT 'Hello, world!'")
#' dbDisconnect(con, shutdown = TRUE)
setMethod(
  "dbConnect", "duckdb_driver",
  function(drv, dbdir = DBDIR_MEMORY, ..., debug = getOption("duckdb.debug", FALSE), read_only = FALSE) {

    check_flag(debug)

    missing_dbdir <- missing(dbdir)
    dbdir <- path.expand(as.character(dbdir))


    # aha, a late comer. let's make a new instance.
    if (!missing_dbdir && dbdir != drv@dbdir) {
      duckdb_shutdown(drv)
      drv <- duckdb(dbdir, read_only)
    }

    duckdb_connection(drv, debug = debug)
  }
)

#' @description
#' `dbDisconnect()` closes a DuckDB database connection, optionally shutting down
#' the associated instance.
#'
#' @param shutdown Set to `TRUE` to shut down the DuckDB database instance that this connection refers to.
#' @rdname duckdb
#' @export
setMethod(
  "dbDisconnect", "duckdb_connection",
  function(conn, ..., shutdown = FALSE) {
    if (!dbIsValid(conn)) {
      warning("Connection already closed.", call. = FALSE)
    }
    .Call(duckdb_disconnect_R, conn@conn_ref)
    if (shutdown) {
      duckdb_shutdown(conn@driver)
    }

    invisible(TRUE)
  }
)

#' @description
#' `duckdb()` creates or reuses a database instance.
#'
#' @return `duckdb()` returns an object of class \linkS4class{duckdb_driver}.
#'
#' @import methods DBI
#' @export
duckdb <- function(dbdir = DBDIR_MEMORY, read_only = FALSE) {
  check_flag(read_only)
  new(
    "duckdb_driver",
    database_ref = .Call(duckdb_startup_R, dbdir, read_only),
    dbdir = dbdir,
    read_only = read_only
  )
}

#' @inheritParams DBI::dbDataType
#' @export
setMethod(
  "dbDataType", "duckdb_driver",
  function(dbObj, obj, ...) {

    if (is.null(obj)) stop("NULL parameter")
    if (is.data.frame(obj)) {
      return(vapply(obj, function(x) dbDataType(dbObj, x), FUN.VALUE = "character"))
    }
    #  else if (int64 && inherits(obj, "integer64")) "BIGINT"
    else if (inherits(obj, "Date")) {
      "DATE"
    } else if (inherits(obj, "difftime")) {
      "TIME"
    } else if (is.logical(obj)) {
      "BOOLEAN"
    } else if (is.integer(obj)) {
      "INTEGER"
    } else if (is.numeric(obj)) {
      "DOUBLE"
    } else if (inherits(obj, "POSIXt")) {
      "TIMESTAMP"
    } else if (is.list(obj) && all(vapply(obj, typeof, FUN.VALUE = "character") == "raw" || is.na(obj))) {
      "BLOB"
    } else {
      "STRING"
    }

  }
)

#' @rdname duckdb_driver-class
#' @inheritParams DBI::dbIsValid
#' @importFrom DBI dbConnect
#' @export
setMethod(
  "dbIsValid", "duckdb_driver",
  function(dbObj, ...) {
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
)

#' @rdname duckdb_driver-class
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "duckdb_driver",
  function(dbObj, ...) {
    list(driver.version = NA, client.version = NA)
  }
)


#' @description
#' `duckdb_shutdown()` shuts down a database instance.
#'
#' @return `dbDisconnect()` and `duckdb_shutdown()` are called for their
#'   side effect.
#' @rdname duckdb
#' @export
duckdb_shutdown <- function(drv) {
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

is_installed <- function(pkg) {
  as.logical(requireNamespace(pkg, quietly = TRUE)) == TRUE
}
