library("testthat")
library("DBI")

#skip_on_cran()
skip_on_os("windows")
skip_if_not_installed("arrow", "5.0.0")
# Skip if parquet is not a capability as an indicator that Arrow is fully installed.
skip_if_not(arrow::arrow_with_parquet(), message = "The installed Arrow is not fully featured, skipping Arrow integration tests")

test_that("we can round trip substrait plans", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))
  dbExecute(con, "CREATE TABLE integers (i INTEGER)")
  dbExecute(con, "INSERT INTO integers VALUES (42)")
  plan <- duckdb::duckdb_get_substrait(con, "select * from integers limit 5")
  result <- duckdb::duckdb_prepare_substrait(con, plan)
  df <- dbFetch(result)
  expect_equal(df$i, 42L)

  result_arrow <- duckdb::duckdb_prepare_substrait(con, plan, TRUE)
  df2 <- as.data.frame(duckdb::duckdb_fetch_arrow(result_arrow))
  expect_equal(df2$i, 42L)
})
