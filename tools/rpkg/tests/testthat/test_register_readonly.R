test_that("we can register a data frame on a read only connection", {
  path = tempfile()
  # create empty database
  con = DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = FALSE)
  DBI::dbDisconnect(con, shutdown = TRUE)

  # reopen database read-only, try to write temp table
  con = DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = TRUE)
  duckdb::duckdb_register(con, "mtcars", mtcars)
})

