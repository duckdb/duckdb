test_that("ADBC driver can create databases, connections, and statements", {
  skip_if_not_installed("adbcdrivermanager")

  drv <- duckdb_adbc()
  expect_s3_class(drv, "duckdb_driver_adbc")

  db <- adbcdrivermanager::local_adbc(
    adbcdrivermanager::adbc_database_init(duckdb_adbc())
  )
  expect_s3_class(db, "duckdb_database_adbc")

  con <- adbcdrivermanager::local_adbc(
    adbcdrivermanager::adbc_connection_init(db)
  )
  expect_s3_class(con, "duckdb_connection_adbc")

  stmt <- adbcdrivermanager::local_adbc(
    adbcdrivermanager::adbc_statement_init(con)
  )
  expect_s3_class(stmt, "duckdb_statement_adbc")

  stream <- adbcdrivermanager::read_adbc(con, "SELECT 1 as one;")
  expect_identical(as.data.frame(stream), data.frame(one = 1L))
})
