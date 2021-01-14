test_that("timezone_out works with a specified timezone", {
  con <- dbConnect(duckdb(), timezone_out = "Etc/GMT+8")
  on.exit(dbDisconnect(con, shutdown = TRUE))

  query <- "SELECT '1970-01-01 12:00:00'::TIMESTAMP AS ts"
  res <- dbGetQuery(con, query)
  expect_equal(res[[1]], as.POSIXct("1970-01-01 04:00:00", tz = "Etc/GMT+8"))
})

test_that("timezone_out works with UTC", {
  con <- dbConnect(duckdb(), timezone_out = "UTC")
  on.exit(dbDisconnect(con, shutdown = TRUE))

  query <- "SELECT '1970-01-01 12:00:00'::TIMESTAMP AS ts"
  res <- dbGetQuery(con, query)
  expect_equal(res[[1]], as.POSIXct("1970-01-01 12:00:00", tz = "UTC"))
})

test_that("timezone_out works with '' and converts to local timezime", {
  withr::local_timezone("America/Vancouver")
  con <- dbConnect(duckdb(), timezone_out = "")
  on.exit(dbDisconnect(con, shutdown = TRUE))

  query <- "SELECT '1970-01-01 12:00:00'::TIMESTAMP AS ts"
  res <- dbGetQuery(con, query)
  expect_equal(res[[1]], as.POSIXct("1970-01-01 04:00:00", tz = "America/Vancouver"))
})

test_that("timezone_out works with a specified local timezone", {
  withr::local_timezone("America/Vancouver")
  con <- dbConnect(duckdb(), timezone_out = Sys.timezone())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  query <- "SELECT '1970-01-01 12:00:00'::TIMESTAMP AS ts"
  res <- dbGetQuery(con, query)
  expect_equal(res[[1]], as.POSIXct("1970-01-01 04:00:00", tz = "America/Vancouver"))
})

test_that("timezone_out gives a warning with invalid timezone, and converts to UTC", {
  expect_warning(con <- dbConnect(duckdb(), timezone_out = "not_a_timezone"))
  on.exit(dbDisconnect(con, shutdown = TRUE))
  expect_equal(con@timezone_out, "UTC")
})

test_that("timezone_out gives a warning with NULL timezone, and converts to UTC", {
  expect_warning(con <- dbConnect(duckdb(), timezone_out = "not_a_timezone"))
  on.exit(dbDisconnect(con, shutdown = TRUE))
  expect_equal(con@timezone_out, "UTC")
})

