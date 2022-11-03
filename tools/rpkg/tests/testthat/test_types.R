test_that("test_all_types() output", {
  skip_on_os("windows")

  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  local_edition(3)
  withr::local_options(digits.secs = 6)

  expect_snapshot({
    as.list(dbGetQuery(con, "SELECT * EXCLUDE (timestamp_ns, timestamp_tz, timestamp_array, timestamptz_array, time_tz, map) FROM test_all_types()"))
  })
})

test_that("test_all_types() output for non-macOS", {
  skip_on_os("windows")
  skip_on_os("mac")

  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  local_edition(3)
  withr::local_options(digits.secs = 6)

  expect_snapshot({
    as.list(dbGetQuery(con, "SELECT timestamp_ns, timestamp_array, timestamptz_array FROM test_all_types()"))
  })
})

test_that("test_all_types() output for macOS", {
  skip_on_os("windows")
  skip_on_os("linux")

  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  local_edition(3)
  withr::local_options(digits.secs = 6)

  expect_snapshot({
    as.list(dbGetQuery(con, "SELECT timestamp_ns, timestamp_array, timestamptz_array FROM test_all_types()"))
  })
})
