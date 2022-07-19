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
#' @param timezone_out The time zone returned to R, defaults to `"UTC"`, which
#'   is currently the only timezone supported by duckdb.
#'   If you want to display datetime values in the local timezone,
#'   set to [Sys.timezone()] or `""`.
#' @param tz_out_convert How to convert timestamp columns to the timezone specified
#'   in `timezone_out`. There are two options: `"with"`, and `"force"`. If `"with"`
#'   is chosen, the timestamp will be returned as it would appear in the specified time zone.
#'   If `"force"` is chosen, the timestamp will have the same clock
#'   time as the timestamp in the database, but with the new time zone.
#' @param config Named list with DuckDB configuration flags
#' @param bigint How 64-bit integers should be returned, default is double/numeric. Set to integer64 for bit64 encoding.
#'
#' @return `dbConnect()` returns an object of class
#'   \linkS4class{duckdb_connection}.
#'
#' @rdname duckdb
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
#' @usage NULL
dbConnect__duckdb_driver <- function(drv, dbdir = DBDIR_MEMORY, ...,
                                     debug = getOption("duckdb.debug", FALSE),
                                     read_only = FALSE,
                                     timezone_out = "UTC",
                                     tz_out_convert = c("with", "force"), config = list(), bigint="numeric") {
  check_flag(debug)
  timezone_out <- check_tz(timezone_out)
  tz_out_convert <- match.arg(tz_out_convert)

  missing_dbdir <- missing(dbdir)
  dbdir <- path.expand(as.character(dbdir))

  # aha, a late comer. let's make a new instance.
  if (!missing_dbdir && dbdir != drv@dbdir) {
    duckdb_shutdown(drv)
    drv <- duckdb(dbdir, read_only, bigint, config)
  }

  conn <- duckdb_connection(drv, debug = debug)
  on.exit(dbDisconnect(conn))

  conn@timezone_out <- timezone_out
  conn@tz_out_convert <- tz_out_convert

  on.exit(NULL)

  rs_on_connection_opened(conn)

  conn
}

#' @rdname duckdb
#' @export
setMethod("dbConnect", "duckdb_driver", dbConnect__duckdb_driver)
