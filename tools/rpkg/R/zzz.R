.onLoad <- function(...) {
  s3_register("dbplyr::dbplyr_edition", "duckdb_connection")
  s3_register("dbplyr::db_connection_describe", "duckdb_connection")
  s3_register("dbplyr::sql_translation", "duckdb_connection")
  s3_register("dbplyr::dbplyr_fill0", "duckdb_connection")
  s3_register("dbplyr::sql_expr_matches", "duckdb_connection")
  s3_register("dbplyr::sql_escape_date", "duckdb_connection")
  s3_register("dbplyr::sql_escape_datetime", "duckdb_connection")
  s3_register("dplyr::tbl", "duckdb_connection")

  dllinfo <- library.dynam("duckdb", "duckdb", lib.loc=NULL, local=FALSE)
  routines <- getDLLRegisteredRoutines( dllinfo)
  duckdb_env <- asNamespace("duckdb")

  invisible(lapply(routines$.Call, function(symbol) {
   assign(symbol$name, symbol, envir=duckdb_env)
  }))
}

