test_that("configuration key value pairs work as expected", {

  # setting nothing or empty list should work
  drv <- duckdb::duckdb()
  duckdb::duckdb_shutdown(drv)

  drv <- duckdb::duckdb(config = list())
  duckdb::duckdb_shutdown(drv)

  # but we should throw an error on non-existent options
  expect_error(duckdb::duckdb(config = list(a = "a")))

  # but setting a legal option is fine
  drv <- duckdb::duckdb(config = list("default_order" = "DESC"))

  # the option actually does something
  con <- dbConnect(drv)
  dbExecute(con, "create table a (i integer)")
  dbExecute(con, "insert into a values (44), (42)")
  res <- dbGetQuery(con, "select i from a order by i")
  dbDisconnect(con)
  expect_equal(res$i, c(44, 42))
  duckdb::duckdb_shutdown(drv)

  # setting a configuration option to a non-string is an error
  expect_error(duckdb::duckdb(config = list("default_order" = 42)))
  expect_error(duckdb::duckdb(config = list("default_order" = c("a", "b"))))

  # setting a configuration option to an unrecognized value
  expect_error(duckdb::duckdb(config = list("default_order" = "asdf")))
})
