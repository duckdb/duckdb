test_that("parquet reader works on the notorious userdata1 file", {
  con <- dbConnect(duckdb::duckdb())
  res <- dbGetQuery(con, "SELECT * FROM parquet_scan('userdata1.parquet')")
  dbDisconnect(con, shutdown = TRUE)
  expect_true(TRUE)
})
