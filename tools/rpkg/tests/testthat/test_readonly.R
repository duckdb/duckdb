test_that("read_only flag and shutdown works as expected", {
  dbdir <- tempfile()

  # 1st: create a db and write some tables

  callr::r(function(dbdir) {
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir, read_only = FALSE) # FALSE is the default
    print(con)
    res <- DBI::dbWriteTable(con, "iris", iris)
    DBI::dbDisconnect(con)
    duckdb::duckdb_shutdown(con@driver)
  }, args = list(dbdir))


  # 2nd: start two parallel read-only references
  drv <- duckdb(dbdir, read_only = TRUE)
  con <- dbConnect(drv)

  res <- dbReadTable(con, "iris")


  # can have another conn on this drv
  con2 <- dbConnect(drv)
  res <- dbReadTable(con2, "iris")

  # con is still alive
  callr::r(function(dbdir) {
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir, read_only = TRUE)
    res <- DBI::dbReadTable(con, "iris")
    DBI::dbDisconnect(con, shutdown = TRUE)
  }, args = list(dbdir))

  # shut down one of them again
  res <- dbReadTable(con, "iris")

  dbDisconnect(con)
  dbDisconnect(con2, shutdown = TRUE)


  # now we can get write access again
  # TODO shutdown
  callr::r(function(dbdir) {
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir, read_only = FALSE) # FALSE is the default
    res <- DBI::dbWriteTable(con, "iris2", iris)
    DBI::dbDisconnect(con)
  }, args = list(dbdir))

  expect_true(TRUE)
})
