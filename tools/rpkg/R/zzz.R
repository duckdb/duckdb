.onLoad <- function(...) {
  register_s3_method("dbplyr", "dbplyr_edition", "duckdb_connection")
  register_s3_method("dbplyr", "db_connection_describe", "duckdb_connection")
  register_s3_method("dbplyr", "sql_translation", "duckdb_connection")
  register_s3_method("dbplyr", "dbplyr_fill0", "duckdb_connection")
  register_s3_method("dbplyr", "sql_expr_matches", "duckdb_connection")
  register_s3_method("dbplyr", "sql_escape_date", "duckdb_connection")
  register_s3_method("dbplyr", "sql_escape_datetime", "duckdb_connection")
}

# From: dbplyr zzz.R (https://github.com/tidyverse/dbplyr/blob/main/R/zzz.R)
register_s3_method <- function(pkg, generic, class, fun = NULL) {
  stopifnot(is.character(pkg), length(pkg) == 1)
  stopifnot(is.character(generic), length(generic) == 1)
  stopifnot(is.character(class), length(class) == 1)

  if (is.null(fun)) {
    fun <- get(paste0(generic, ".", class), envir = parent.frame())
  } else {
    stopifnot(is.function(fun))
  }

  if (pkg %in% loadedNamespaces()) {
    registerS3method(generic, class, fun, envir = asNamespace(pkg))
  }

  # Always register hook in case package is later unloaded & reloaded
  setHook(
    packageEvent(pkg, "onLoad"),
    function(...) {
      registerS3method(generic, class, fun, envir = asNamespace(pkg))
    }
  )
}

# From: https://github.com/DyfanJones/noctua
# get parent pkg function and method
pkg_method <- function(fun, pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(fun,' requires the ', pkg,' package, please install it first and try again',
         call. = FALSE)}
  fun_name <- utils::getFromNamespace(fun, pkg)
  return(fun_name)
}

