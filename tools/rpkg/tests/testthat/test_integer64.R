# this tests both retrieval and scans
test_that("we can roundtrip an integer64", {
  skip_if_not_installed("bit64")

  con <- dbConnect(duckdb::duckdb(bigint="integer64"))
  on.exit(dbDisconnect(con, shutdown = TRUE))
  df <- data.frame(a = bit64::as.integer64(42), b = bit64::as.integer64(-42), c = bit64::as.integer64(NA))

  duckdb::duckdb_register(con, "df", df)

  res <- dbReadTable(con, "df")
  expect_identical(df, res)
})
