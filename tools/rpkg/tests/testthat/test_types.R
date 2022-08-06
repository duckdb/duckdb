test_that("test_all_types() output", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  local_edition(3)
  rlang::local_options(digits.secs = 6)

  expect_snapshot({
    as.list(dbGetQuery(con, "SELECT * EXCLUDE (timestamp_tz, time_tz, uuid, interval, json, map) FROM test_all_types()"))
  })
})
