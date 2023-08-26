test_that("fractional seconds can be roundtripped", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  df <- data.frame(a = as.POSIXct(1.234567 + (1:100) * 1e-6, tz = "UTC"))
  dbWriteTable(con, "df", df)
  df_out <- dbReadTable(con, "df")
  expect_equal(df_out, df)
})
