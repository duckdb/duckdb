drv <- duckdb()
reg.finalizer(drv@database_ref, function(x) rapi_shutdown(x))

# remotes::install_github("r-dbi/dblog")
# Then, use DBItest::test_some() to see the DBI calls emitted by the tests
#
# This call must stay here, otherwise DBItest::test_some() doesn't work
DBItest::make_context(
  drv,
  # dblog::dblog(drv),
  list(debug = FALSE),
  tweaks = DBItest::tweaks(
    omit_blob_tests = FALSE,
    temporary_tables = FALSE,
    placeholder_pattern = "?",
    timestamp_cast = function(x) sprintf("CAST('%s' AS TIMESTAMP)", x),
    date_cast = function(x) sprintf("CAST('%s' AS DATE)", x),
    time_cast = function(x) sprintf("CAST('%s' AS TIME)", x),
    blob_cast = function(x) sprintf("%s::BLOB", x)
  ),
  name = "duckdb"
)
