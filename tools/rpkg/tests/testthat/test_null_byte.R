test_that("test null bytes in strings", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))
  df <- dbGetQuery(con, "SELECT chr(0)")
  expect_error(x <- df[[1]][1])

  df <- dbGetQuery(con, "SELECT chr(0)")
  expect_error(sort(df[[1]])
})
