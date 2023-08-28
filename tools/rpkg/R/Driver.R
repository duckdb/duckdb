DBDIR_MEMORY <- ":memory:"

check_flag <- function(x) {
  if (is.null(x) || length(x) != 1 || is.na(x) || !is.logical(x)) {
    stop("flags need to be scalar logicals")
  }
}

extptr_str <- function(e, n = 5) {
  x <- rapi_ptr_to_str(e)
  substr(x, nchar(x) - n + 1, nchar(x))
}

drv_to_string <- function(drv) {
  if (!is(drv, "duckdb_driver")) {
    stop("pass a duckdb_driver object")
  }
  sprintf("<duckdb_driver %s dbdir='%s' read_only=%s bigint=%s>", extptr_str(drv@database_ref), drv@dbdir, drv@read_only, drv@bigint)
}

#' @description
#' `duckdb()` creates or reuses a database instance.
#'
#' @return `duckdb()` returns an object of class \linkS4class{duckdb_driver}.
#'
#' @import methods DBI
#' @export
duckdb <- function(dbdir = DBDIR_MEMORY, read_only = FALSE, bigint = "numeric", config = list()) {
  check_flag(read_only)

  switch(bigint,
    numeric = {
      # fine
    },
    integer64 = {
      if (!is_installed("bit64")) {
        stop("bit64 package is required for integer64 support")
      }
    },
    stop(paste("Unsupported bigint configuration", bigint))
  )

  # R packages are not allowed to write extensions into home directory, so use R_user_dir instead
  if (!("extension_directory" %in% names(config))) {
    config["extension_directory"] <- tools::R_user_dir("duckdb", "data")
  }

  new(
    "duckdb_driver",
    database_ref = rapi_startup(dbdir, read_only, config),
    dbdir = dbdir,
    read_only = read_only,
    bigint = bigint
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
  rapi_shutdown(drv@database_ref)
  invisible(TRUE)
}

#' @description
#' Return an [adbcdrivermanager::adbc_driver()] for use with Arrow Database
#' Connectivity via the adbcdrivermanager package.
#'
#' @return An object of class "adbc_driver"
#' @rdname duckdb
#' @export
#' @examplesIf requireNamespace("adbcdrivermanager", quietly = TRUE)
#' library(adbcdrivermanager)
#' with_adbc(db <- adbc_database_init(duckdb_adbc()), {
#'   as.data.frame(read_adbc(db, "SELECT 1 as one;"))
#' })
duckdb_adbc <- function() {
  init_func <- structure(rapi_adbc_init_func(), class = "adbc_driver_init_func")
  adbcdrivermanager::adbc_driver(init_func, subclass = "duckdb_driver_adbc")
}

# Registered in zzz.R
adbc_database_init.duckdb_driver_adbc <- function(driver, ...) {
  adbcdrivermanager::adbc_database_init_default(
    driver,
    list(...),
    subclass = "duckdb_database_adbc"
  )
}

adbc_connection_init.duckdb_database_adbc <- function(database, ...) {
  adbcdrivermanager::adbc_connection_init_default(
    database,
    list(...),
    subclass = "duckdb_connection_adbc"
  )
}

adbc_statement_init.duckdb_connection_adbc <- function(connection, ...) {
  adbcdrivermanager::adbc_statement_init_default(
    connection,
    list(...),
    subclass = "duckdb_statement_adbc"
  )
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
