DBItest::make_context(
  duckdb::duckdb(),
  list(debug = F),
  tweaks = DBItest::tweaks(
    omit_blob_tests = TRUE,
    temporary_tables = FALSE,
    timestamp_cast = function(x) sprintf("CAST('%s' AS TIMESTAMP)", x),
    date_cast = function(x) sprintf("CAST('%s' AS DATE)", x),
    time_cast = function(x) sprintf("CAST('%s' AS TIME)", x)),
  name = "duckdb"
)
