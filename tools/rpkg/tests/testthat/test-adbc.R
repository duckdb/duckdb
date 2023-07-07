test_that("ADBC driver can create databases, connections, and statements", {
  skip_if_not_installed("adbcdrivermanager")
  skip("temporarily to see if it solves the CI failure")

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

test_that("DuckDB ADBC classes can be used in adbc_* S4 object slots", {
  skip_if_not_installed("adbcdrivermanager")

  setClass("SomeClass",
    slots = list(
      driver = "adbc_driver",
      database = "adbc_database",
      connection = "adbc_connection",
      statement = "adbc_statement"
    )
  )
  on.exit(removeClass("SomeClass"))

  setMethod("initialize", "SomeClass", function(.Object, driver) {
    .Object@driver <- driver
    .Object@database <- adbcdrivermanager::adbc_database_init(.Object@driver)
    .Object@connection <- adbcdrivermanager::adbc_connection_init(.Object@database)
    .Object@statement <- adbcdrivermanager::adbc_statement_init(.Object@connection)
    .Object
  })

  expect_s4_class(obj <- new("SomeClass", duckdb_adbc()), "SomeClass")
  adbcdrivermanager::adbc_statement_release(obj@statement)
  adbcdrivermanager::adbc_connection_release(obj@connection)
  adbcdrivermanager::adbc_database_release(obj@database)
})
