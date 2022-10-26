skip_on_cran()
skip_on_os("windows")
skip_if_not_installed("arrow", "5.0.0")
# Skip if parquet is not a capability as an indicator that Arrow is fully installed.
skip_if_not(arrow::arrow_with_parquet(), message = "The installed Arrow is not fully featured, skipping Arrow integration tests")

test_that("duckdb_fetch_arrow() test table over vector size", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("CREATE table test as select range a from range(10000);"))
  dbExecute(con, "INSERT INTO  test VALUES(NULL);")
  arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow = TRUE))
  duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

  expect_equal(dbGetQuery(con, "SELECT * from testarrow"), dbGetQuery(con, "SELECT * from test"))

  duckdb::duckdb_unregister_arrow(con, "testarrow")
})

test_that("duckdb_fetch_arrow() empty table", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("CREATE TABLE test (a  INTEGER)"))

  arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow = TRUE))
  duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

  expect_equal(dbGetQuery(con, "SELECT * from testarrow"), dbGetQuery(con, "SELECT * from test"))

  duckdb::duckdb_unregister_arrow(con, "testarrow")
})

test_that("duckdb_fetch_arrow() table with only nulls", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("CREATE TABLE test (a  INTEGER)"))

  dbExecute(con, "INSERT INTO  test VALUES(NULL);")
  arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow = TRUE))
  duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

  expect_equal(dbGetQuery(con, "SELECT * from testarrow"), dbGetQuery(con, "SELECT * from test"))

  duckdb::duckdb_unregister_arrow(con, "testarrow")
})

test_that("duckdb_fetch_arrow() table with prepared statement", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("CREATE TABLE test (a  INTEGER)"))
  dbExecute(con, paste0("PREPARE s1 AS INSERT INTO test VALUES ($1), ($2 / 2)"))
  for (value in 1:1500) {
    dbExecute(con, sprintf("EXECUTE s1 (%d, %d);", value, value * 2))
  }
  arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow = TRUE))
  duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

  expect_equal(dbGetQuery(con, "SELECT * from testarrow"), dbGetQuery(con, "SELECT * from test"))

  duckdb::duckdb_unregister_arrow(con, "testarrow")
})


test_that("duckdb_fetch_arrow() record_batch_reader ", {
  skip_if_not_installed("arrow", "4.0.1")
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("CREATE table t as select range a from range(3000);"))
  res <- dbSendQuery(con, "SELECT * FROM t", arrow = TRUE)
  record_batch_reader <- duckdb::duckdb_fetch_record_batch(res,1024)
  cur_batch <- record_batch_reader$read_next_batch()
  expect_equal(1024, cur_batch$num_rows)

  cur_batch <- record_batch_reader$read_next_batch()
  expect_equal(1024, cur_batch$num_rows)

  cur_batch <- record_batch_reader$read_next_batch()
  expect_equal(952, cur_batch$num_rows)

  cur_batch <- record_batch_reader$read_next_batch()
  expect_equal(NULL, cur_batch)
})

test_that("duckdb_fetch_arrow() record_batch_reader multiple vectors per chunk", {
  skip_if_not_installed("arrow", "4.0.1")
  con <- dbConnect(duckdb::duckdb())
  dbExecute(con, paste0("CREATE table t as select range a from range(5000);"))
  res <- dbSendQuery(con, "SELECT * FROM t", arrow = TRUE)
  record_batch_reader <- duckdb::duckdb_fetch_record_batch(res, 2048)
  cur_batch <- record_batch_reader$read_next_batch()
  expect_equal(2048, cur_batch$num_rows)

  cur_batch <- record_batch_reader$read_next_batch()
  expect_equal(2048, cur_batch$num_rows)

  cur_batch <- record_batch_reader$read_next_batch()
  expect_equal(904, cur_batch$num_rows)

  record_batch_reader$read_next_batch()

  dbDisconnect(con, shutdown = T)
})

test_that("record_batch_reader and table error", {
  skip_if_not_installed("arrow", "4.0.1")
  con <- dbConnect(duckdb::duckdb())
  dbExecute(con, paste0("CREATE table t as select range a from range(5000);"))
  res <- dbSendQuery(con, "SELECT * FROM t", arrow = TRUE)
  expect_error(duckdb::duckdb_fetch_record_batch(res, 0))
  expect_error(duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow = TRUE),0))

  dbDisconnect(con, shutdown = T)
})


test_that("duckdb_fetch_arrow() record_batch_reader defaultparamenter", {
  skip_if_not_installed("arrow", "4.0.1")
  con <- dbConnect(duckdb::duckdb())
  dbExecute(con, paste0("CREATE table t as select range a from range(5000);"))
  res <- dbSendQuery(con, "SELECT * FROM t", arrow = TRUE)
  record_batch_reader <- duckdb::duckdb_fetch_record_batch(res)
  cur_batch <- record_batch_reader$read_next_batch()
  expect_equal(5000, cur_batch$num_rows)

  record_batch_reader$read_next_batch()

  dbDisconnect(con, shutdown = T)
})

test_that("duckdb_fetch_arrow() record_batch_reader Read Table", {
  skip_if_not_installed("arrow", "4.0.1")
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("CREATE table t as select range a from range(3000);"))
  res <- dbSendQuery(con, "SELECT * FROM t", arrow = TRUE)
  record_batch_reader <- duckdb::duckdb_fetch_record_batch(res)
  arrow_table <- record_batch_reader$read_table()
  expect_equal(3000, arrow_table$num_rows)
})
