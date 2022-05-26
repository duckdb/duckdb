.onLoad <- function(...) {
  s3_register("dbplyr::dbplyr_edition", "duckdb_connection")
  s3_register("dbplyr::db_connection_describe", "duckdb_connection")
  s3_register("dbplyr::sql_translation", "duckdb_connection")
  s3_register("dbplyr::dbplyr_fill0", "duckdb_connection")
  s3_register("dbplyr::sql_expr_matches", "duckdb_connection")
  s3_register("dbplyr::sql_escape_date", "duckdb_connection")
  s3_register("dbplyr::sql_escape_datetime", "duckdb_connection")
  s3_register("dplyr::tbl", "duckdb_connection")

  con <- dbConnect__duckdb_driver(duckdb())
  on.exit(dbDisconnect__duckdb_connection(con, shutdown = TRUE), add = TRUE)

  rs <- dbSendQuery__duckdb_connection_character(con, "SELECT * FROM duckdb_keywords();")
  on.exit(dbClearResult__duckdb_result(rs), add = TRUE)
  the$reserved_words <- dbFetch__duckdb_result(rs)[[1]]

  invisible()
}
