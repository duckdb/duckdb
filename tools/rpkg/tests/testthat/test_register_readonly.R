test_that("we can register a data frame on a read only connection", {
  path <- tempfile()
  # create empty database
  con <- dbConnect(duckdb(), dbdir = path, read_only = FALSE)
  dbDisconnect(con, shutdown = TRUE)

  # reopen database read-only, try to write temp table
  con <- dbConnect(duckdb(), dbdir = path, read_only = TRUE)
  expect_true(duckdb_register(con, "mtcars", mtcars))
  dbDisconnect(con, shutdown = TRUE)
})
