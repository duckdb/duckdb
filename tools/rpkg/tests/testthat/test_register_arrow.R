library("testthat")
library("DBI")

test_that("duckdb_register_arrow() works", {
  skip_if_not_installed("arrow", "4.0.1")
  con <- dbConnect(duckdb::duckdb())
  res <- arrow::read_parquet("userdata1.parquet", as_data_frame=FALSE)
  duckdb::duckdb_register_arrow(con, "myreader", res)
  res1 <- dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 10")
  res2 <- dbGetQuery(con, "SELECT first_name, last_name FROM parquet_scan('userdata1.parquet') LIMIT 10")
  expect_true(identical(res1, res2))
  # we can re-read
  res3 <- dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 10")
  expect_true(identical(res2, res3))
  duckdb::duckdb_unregister_arrow(con, "myreader")
  # cant read after unregister
  expect_error(dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 10"))

#   # cant register something non-arrow
#   expect_error(duckdb_register_arrow(con, "asdf", data.frame()))

  dbDisconnect(con, shutdown = T)
})
