test_that("test_all_types() output", {
  skip_on_os("windows")

  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  local_edition(3)
  withr::local_options(digits.secs = 6)

  expect_snapshot({
    as.list(dbGetQuery(con, "SELECT * EXCLUDE (timestamp_tz, time_tz, uuid, interval, json, map) FROM test_all_types()"))
  })
})
