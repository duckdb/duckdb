test_that("parquet reader works on the notorious userdata1 file", {
  con <- dbConnect(duckdb())
  res <- dbGetQuery(con, "SELECT * FROM parquet_scan('data/userdata1.parquet')")
  dbDisconnect(con, shutdown = TRUE)
  expect_true(TRUE)
})

test_that("parquet reader works with the binary as string flag", {
  con <- dbConnect(duckdb())
  res <- dbGetQuery(con, "SELECT typeof(#1) FROM parquet_scan('data/binary_string.parquet',binary_as_string=true) limit 1")
  expect_true(res[1] == "VARCHAR")
  dbDisconnect(con, shutdown = TRUE)
})
