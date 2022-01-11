DBDIR_MEMORY <- ":memory:"

check_flag <- function(x) {
  if (is.null(x) || length(x) != 1 || is.na(x) || !is.logical(x)) {
    stop("flags need to be scalar logicals")
  }
}

extptr_str <- function(e, n = 5) {
  x <- .Call(`_duckdb_ptr_to_str`, e)
  substr(x, nchar(x) - n + 1, nchar(x))
}

drv_to_string <- function(drv) {
  if (!is(drv, "duckdb_driver")) {
    stop("pass a duckdb_driver object")
  }
  sprintf("<duckdb_driver %s dbdir='%s' read_only=%s>", extptr_str(drv@database_ref), drv@dbdir, drv@read_only)
}

#' @description
#' `duckdb()` creates or reuses a database instance.
#'
#' @return `duckdb()` returns an object of class \linkS4class{duckdb_driver}.
#'
#' @import methods DBI
#' @export
duckdb <- function(dbdir = DBDIR_MEMORY, read_only = FALSE, config=list()) {
  check_flag(read_only)
  new(
    "duckdb_driver",
    database_ref = startup_R(dbdir, read_only, config),
    dbdir = dbdir,
    read_only = read_only
  )
}

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
  .Call(`_duckdb_shutdown_R`, drv@database_ref)
  invisible(TRUE)
}

is_installed <- function(pkg) {
  as.logical(requireNamespace(pkg, quietly = TRUE)) == TRUE
}

check_tz <- function(timezone) {

  if (!is.null(timezone) && timezone == "") {
    return(Sys.timezone())
  }

  if (is.null(timezone) || !timezone %in% OlsonNames()) {
    warning(
      "Invalid time zone '", timezone, "', ",
      "falling back to UTC.\n",
      "Set the `timezone_out` argument to a valid time zone.\n",
      call. = FALSE
    )
    return("UTC")
  }

  timezone
}
