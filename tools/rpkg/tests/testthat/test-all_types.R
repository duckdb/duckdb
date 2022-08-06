test_that("test_all_types() output", {
  local_edition(3)
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  expect_snapshot({
    unclass(dbGetQuery(con, "SELECT * EXCLUDE (timestamp_tz, time_tz, uuid, interval, json, map) FROM test_all_types()"))
  })
})
