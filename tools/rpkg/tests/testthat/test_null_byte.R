test_that("test null bytes in strings", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))
  expect_error(dbGetQuery(con, "SELECT chr(0)"))
})
