.onLoad <- function(...) {
  s3_register("dbplyr::dbplyr_edition", "duckdb_connection")
  s3_register("dbplyr::db_connection_describe", "duckdb_connection")
  s3_register("dbplyr::sql_translation", "duckdb_connection")
  s3_register("dbplyr::sql_expr_matches", "duckdb_connection")
  s3_register("dbplyr::sql_escape_date", "duckdb_connection")
  s3_register("dbplyr::sql_escape_datetime", "duckdb_connection")
  s3_register("dplyr::tbl", "duckdb_connection")
  s3_register("adbcdrivermanager::adbc_database_init", "duckdb_driver_adbc")
  s3_register("adbcdrivermanager::adbc_connection_init", "duckdb_database_adbc")
  s3_register("adbcdrivermanager::adbc_statement_init", "duckdb_connection_adbc")

  invisible()
}
