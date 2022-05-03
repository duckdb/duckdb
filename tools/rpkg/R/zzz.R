.onLoad <- function(libname, pkgname) {
  s3_register("dbplyr::dbplyr_edition", "duckdb_connection")
  s3_register("dbplyr::db_connection_describe", "duckdb_connection")
  s3_register("dbplyr::sql_translation", "duckdb_connection")
  s3_register("dbplyr::dbplyr_fill0", "duckdb_connection")
  s3_register("dbplyr::sql_expr_matches", "duckdb_connection")
  s3_register("dbplyr::sql_escape_date", "duckdb_connection")
  s3_register("dbplyr::sql_escape_datetime", "duckdb_connection")
  s3_register("dplyr::tbl", "duckdb_connection")

  duckdb_env <- asNamespace("duckdb")

  # Extract path to DLL to a separate variable
  # (a complex expression inside dyn.load() crashes RStudio)
  dllinfo <- duckdb_env$.__NAMESPACE__.$DLLs$duckdb
  path <- unclass(dllinfo)$path
  dyn.load(path, local = TRUE)
  NULL
}

