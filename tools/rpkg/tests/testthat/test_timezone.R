test_that("timezone_out works with default", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  query <- "SELECT '1970-01-01 12:00:00'::TIMESTAMP AS ts"
  res <- dbGetQuery(con, query)
  expect_equal(res[[1]], as.POSIXct("1970-01-01 12:00:00", tz = "UTC"))
})

test_that("timezone_out works with UTC specified", {
  con <- dbConnect(duckdb(), timezone_out = "UTC")
  on.exit(dbDisconnect(con, shutdown = TRUE))

  query <- "SELECT '1970-01-01 12:00:00'::TIMESTAMP AS ts"
  res <- dbGetQuery(con, query)
  expect_equal(res[[1]], as.POSIXct("1970-01-01 12:00:00", tz = "UTC"))
})

test_that("timezone_out works with a specified timezone", {
  con <- dbConnect(duckdb(), timezone_out = "Pacific/Tahiti")
  on.exit(dbDisconnect(con, shutdown = TRUE))

  query <- "SELECT '1970-01-01 12:00:00'::TIMESTAMP AS ts"
  res <- dbGetQuery(con, query)
  expect_equal(res[[1]], as.POSIXct("1970-01-01 02:00:00", tz = "Pacific/Tahiti"))
})

test_that("timezone_out works with '' and converts to local timezime", {
  unlockBinding(".sys.timezone", baseenv())
  withr::local_timezone("Pacific/Tahiti")
  con <- dbConnect(duckdb(), timezone_out = "")
  on.exit(dbDisconnect(con, shutdown = TRUE))

  query <- "SELECT '1970-01-01 12:00:00'::TIMESTAMP AS ts"
  res <- dbGetQuery(con, query)
  expect_equal(res[[1]], as.POSIXct("1970-01-01 02:00:00", tz = "Pacific/Tahiti"))
})

test_that("timezone_out works with Sys.timezone", {
  unlockBinding(".sys.timezone", baseenv())
  withr::local_timezone("Pacific/Tahiti")
  con <- dbConnect(duckdb(), timezone_out = Sys.timezone())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  query <- "SELECT '1970-01-01 12:00:00'::TIMESTAMP AS ts"
  res <- dbGetQuery(con, query)
  expect_equal(res[[1]], as.POSIXct("1970-01-01 02:00:00", tz = "Pacific/Tahiti"))
})

test_that("timezone_out works with UTC and tz_out_convert = 'force'", {
  con <- dbConnect(duckdb(), timezone_out = "UTC", tz_out_convert = "force")
  on.exit(dbDisconnect(con, shutdown = TRUE))

  query <- "SELECT '1970-01-01 12:00:00'::TIMESTAMP AS ts"
  res <- dbGetQuery(con, query)
  expect_equal(res[[1]], as.POSIXct("1970-01-01 12:00:00", tz = "UTC"))
})

test_that("timezone_out works with a specified timezone and tz_out_convert = 'force'", {
  con <- dbConnect(duckdb(), timezone_out = "Pacific/Tahiti", tz_out_convert = "force")
  on.exit(dbDisconnect(con, shutdown = TRUE))

  query <- "SELECT '1970-01-01 12:00:00'::TIMESTAMP AS ts"
  res <- dbGetQuery(con, query)
  expect_equal(res[[1]], as.POSIXct("1970-01-01 12:00:00", tz = "Pacific/Tahiti"))
})

test_that("timezone_out works with '' and tz_out_convert = 'force': forces local timezime", {
  unlockBinding(".sys.timezone", baseenv())
  withr::local_timezone("Pacific/Tahiti")
  con <- dbConnect(duckdb(), timezone_out = "", tz_out_convert = "force")
  on.exit(dbDisconnect(con, shutdown = TRUE))

  query <- "SELECT '1970-01-01 12:00:00'::TIMESTAMP AS ts"
  res <- dbGetQuery(con, query)
  expect_equal(res[[1]], as.POSIXct("1970-01-01 12:00:00", tz = "Pacific/Tahiti"))
})

test_that("timezone_out works with a specified local timezone and tz_out_convert = 'force': forces local timezime", {
  unlockBinding(".sys.timezone", baseenv())
  withr::local_timezone("Pacific/Tahiti")
  con <- dbConnect(duckdb(), timezone_out = Sys.timezone(), tz_out_convert = "force")
  on.exit(dbDisconnect(con, shutdown = TRUE))

  query <- "SELECT '1970-01-01 12:00:00'::TIMESTAMP AS ts"
  res <- dbGetQuery(con, query)
  expect_equal(res[[1]], as.POSIXct("1970-01-01 12:00:00", tz = "Pacific/Tahiti"))
})

test_that("timezone_out gives a warning with invalid timezone, and converts to UTC", {
  expect_warning(con <- dbConnect(duckdb(), timezone_out = "not_a_timezone"))
  on.exit(dbDisconnect(con, shutdown = TRUE))
  expect_equal(con@timezone_out, "UTC")
})

test_that("timezone_out gives a warning with NULL timezone, and converts to UTC", {
  expect_warning(con <- dbConnect(duckdb(), timezone_out = NULL))
  on.exit(dbDisconnect(con, shutdown = TRUE))
  expect_equal(con@timezone_out, "UTC")
})

test_that("dbConnect fails when tz_out_convert is misspecified", {
  drv <- duckdb()
  on.exit(duckdb_shutdown(drv))

  expect_error(dbConnect(drv, tz_out_convert = "nope"))
})
