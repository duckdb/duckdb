test_that("dbGetInfo returns something meaningful", {
  dbdir <- tempfile()
  drv <- duckdb(dbdir)

  info_drv <- dbGetInfo(drv)
  expect_equal(info_drv$dbname, dbdir)
  expect_true(grepl("\\d+\\.\\d+\\.\\d+", info_drv$client.version))
  expect_true(grepl("\\d+\\.\\d+\\.\\d+", info_drv$driver.version))

  con <- dbConnect(drv)
  on.exit(dbDisconnect(con, shutdown = TRUE))
  info_con <- dbGetInfo(con)
  expect_equal(info_con$dbname, dbdir)
  expect_true(grepl("\\d+\\.\\d+\\.\\d+", info_con$db.version))
})
