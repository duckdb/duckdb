skip_on_cran()
skip_on_os("windows")
skip_if_not_installed("arrow", "5.0.0")
# Skip if parquet is not a capability as an indicator that Arrow is fully installed.
skip_if_not(arrow::arrow_with_parquet(), message = "The installed Arrow is not fully featured, skipping Arrow integration tests")

library("arrow")

test_that("duckdb_register_arrow() works", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  res <- arrow::read_parquet("data/userdata1.parquet", as_data_frame = FALSE)
  duckdb::duckdb_register_arrow(con, "myreader", res)
  res1 <- dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 10")
  res2 <- dbGetQuery(con, "SELECT first_name, last_name FROM parquet_scan('data/userdata1.parquet') LIMIT 10")
  expect_true(identical(res1, res2))
  # we can re-read
  res3 <- dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 10")
  expect_true(identical(res2, res3))
  duckdb::duckdb_unregister_arrow(con, "myreader")
  # cant read after unregister
  expect_error(dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 100"))

  #   # cant register something non-arrow
  #   expect_error(duckdb_register_arrow(con, "asdf", data.frame()))
})

test_that("duckdb_register_arrow() works with record_batch_readers", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  res <- arrow::read_parquet("data/userdata1.parquet", as_data_frame = TRUE)
  res <- arrow::record_batch(res)
  duckdb::duckdb_register_arrow(con, "myreader", res)
  res1 <- dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 10")
  res2 <- dbGetQuery(con, "SELECT first_name, last_name FROM parquet_scan('data/userdata1.parquet') LIMIT 10")
  expect_true(identical(res1, res2))
  # we can re-read
  res3 <- dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 10")
  expect_true(identical(res2, res3))
  duckdb::duckdb_unregister_arrow(con, "myreader")
  # cant read after unregister
  expect_error(dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 100"))
})

test_that("duckdb_register_arrow() works with scanner", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  res <- arrow::read_parquet("data/userdata1.parquet", as_data_frame = FALSE)
  res <- arrow::Scanner$create(res)
  duckdb::duckdb_register_arrow(con, "myreader", res)
  res1 <- dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 10")
  res2 <- dbGetQuery(con, "SELECT first_name, last_name FROM parquet_scan('data/userdata1.parquet') LIMIT 10")
  expect_true(identical(res1, res2))
  # we can re-read
  res3 <- dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 10")
  expect_true(identical(res2, res3))
  duckdb::duckdb_unregister_arrow(con, "myreader")
  # cant read after unregister
  expect_error(dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 100"))
})


test_that("duckdb_register_arrow() works with datasets", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Registering a dataset + aggregation
  ds <- arrow::open_dataset("data/userdata1.parquet")
  duckdb::duckdb_register_arrow(con, "mydatasetreader", ds)
  res1 <- dbGetQuery(con, "SELECT count(*) FROM mydatasetreader")
  res2 <- dbGetQuery(con, "SELECT count(*) FROM parquet_scan('data/userdata1.parquet')")
  expect_true(identical(res1, res2))
  # we can read with > 3 cores
  dbExecute(con, "PRAGMA threads=4")
  res3 <- dbGetQuery(con, "SELECT count(*) FROM mydatasetreader")
  expect_true(identical(res2, res3))
  duckdb::duckdb_unregister_arrow(con, "mydatasetreader")
})


test_that("duckdb_register_arrow() works with datasets and async arrow scanner", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Registering a dataset + aggregation
  ds <- arrow::open_dataset("data/userdata1.parquet")
  duckdb::duckdb_register_arrow(con, "mydatasetreader", ds)
  res1 <- dbGetQuery(con, "SELECT count(*) FROM mydatasetreader")
  res2 <- dbGetQuery(con, "SELECT count(*) FROM parquet_scan('data/userdata1.parquet')")
  expect_true(identical(res1, res2))
  # we can read with > 3 cores
  dbExecute(con, "PRAGMA threads=4")
  res3 <- dbGetQuery(con, "SELECT count(*) FROM mydatasetreader")
  expect_true(identical(res2, res3))
  duckdb::duckdb_unregister_arrow(con, "mydatasetreader")
})


test_that("duckdb_register_arrow() performs projection pushdown", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Registering a dataset + aggregation
  ds <- arrow::open_dataset("data/userdata1.parquet")
  duckdb::duckdb_register_arrow(con, "mydatasetreader", ds)

  res1 <- dbGetQuery(con, "SELECT last_name, salary, first_name FROM mydatasetreader")
  res2 <- dbGetQuery(con, "SELECT last_name, salary, first_name FROM parquet_scan('data/userdata1.parquet')")
  expect_true(identical(res1, res2))
  # we can read with > 3 cores
  dbExecute(con, "PRAGMA threads=4")
  res3 <- dbGetQuery(con, "SELECT last_name, salary, first_name FROM mydatasetreader")
  expect_true(identical(res2, res3))
  duckdb::duckdb_unregister_arrow(con, "mydatasetreader")
})

test_that("duckdb_register_arrow() performs selection pushdown", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # Registering a dataset + aggregation
  ds <- arrow::open_dataset("data/userdata1.parquet")
  duckdb::duckdb_register_arrow(con, "mydatasetreader", ds)

  res1 <- dbGetQuery(con, "SELECT last_name, first_name FROM mydatasetreader where salary > 130000")
  res2 <- dbGetQuery(con, "SELECT last_name, first_name FROM parquet_scan('data/userdata1.parquet')  where salary > 130000")
  expect_true(identical(res1, res2))
  # we can read with > 3 cores
  dbExecute(con, "PRAGMA threads=4")
  res3 <- dbGetQuery(con, "SELECT last_name, first_name FROM mydatasetreader where salary > 130000")
  expect_true(identical(res2, res3))
  duckdb::duckdb_unregister_arrow(con, "mydatasetreader")
})


numeric_operators <- function(data_type) {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("CREATE TABLE test (a ", data_type, ", b ", data_type, ", c ", data_type, ")"))
  dbExecute(con, "INSERT INTO  test VALUES (1,1,1),(10,10,10),(100,10,100),(NULL,NULL,NULL)")
  arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow = TRUE))
  duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

  # Try ==
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a =1")[[1]], 1)
  # Try >
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >1")[[1]], 2)
  # Try >=
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >=10")[[1]], 2)
  # Try <
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <10")[[1]], 1)
  # Try <=
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <=10")[[1]], 2)

  # Try Is Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NULL")[[1]], 1)
  # Try Is Not Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NOT NULL")[[1]], 3)

  # Try And
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a=10 and b =1")[[1]], 0)
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a =100 and b = 10 and c = 100")[[1]], 1)
  # Try Or
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a = 100 or b =1")[[1]], 2)

  duckdb::duckdb_unregister_arrow(con, "testarrow")
}



test_that("duckdb_register_arrow() performs selection pushdown numeric types", {
  numeric_types <- c(
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
    "FLOAT", "DOUBLE", "HUGEINT"
  )

  for (data_type in numeric_types) {
    numeric_operators(data_type)
  }
})

test_that("duckdb_register_arrow() performs selection pushdown decimal types", {
  numeric_types <- c("DECIMAL(4,1)", "DECIMAL(9,1)", "DECIMAL(18,4)", "DECIMAL(30,12)")
  for (data_type in numeric_types) {
    numeric_operators(data_type)
  }
})

test_that("duckdb_register_arrow() performs selection pushdown varchar type", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("CREATE TABLE test (a  VARCHAR, b VARCHAR, c VARCHAR)"))
  dbExecute(con, "INSERT INTO  test VALUES ('1','1','1'),('10','10','10'),('100','10','100'),(NULL,NULL,NULL)")
  arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow = TRUE))
  duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

  # Try ==
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a ='1'")[[1]], 1)
  # Try >
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >'1'")[[1]], 2)
  # Try >=
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >='10'")[[1]], 2)
  # Try <
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <'10'")[[1]], 1)
  # Try <=
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <='10'")[[1]], 2)

  # Try Is Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NULL")[[1]], 1)
  # Try Is Not Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NOT NULL")[[1]], 3)

  # Try And
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a='10' and b ='1'")[[1]], 0)
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a ='100' and b = '10' and c = '100'")[[1]], 1)
  # Try Or
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a = '100' or b ='1'")[[1]], 2)

  duckdb::duckdb_unregister_arrow(con, "testarrow")
})

test_that("duckdb_register_arrow() performs selection pushdown bool type", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("CREATE TABLE test (a  BOOL, b BOOL)"))
  dbExecute(con, "INSERT INTO  test VALUES (TRUE,TRUE),(TRUE,FALSE),(FALSE,TRUE),(NULL,NULL)")
  arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow = TRUE))
  duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

  # Try ==
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a =True")[[1]], 2)

  # Try Is Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NULL")[[1]], 1)
  # Try Is Not Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NOT NULL")[[1]], 3)

  # Try And
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a=True and b =True")[[1]], 1)
  # Try Or
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a = True or b =True")[[1]], 3)

  duckdb::duckdb_unregister_arrow(con, "testarrow")
})

test_that("duckdb_register_arrow() performs selection pushdown time type", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("CREATE TABLE test (a  TIME, b TIME, c TIME)"))

  time <- structure(c(60, 600, 3600, NA), class = "difftime", units = "secs")
  arrow_table <- arrow::arrow_table(a = time, b = time, c = time)
  duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

  # Try ==
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a ='00:01:00'")[[1]], 1)
  # # Try >
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >'00:01:00'")[[1]], 2)
  # # Try >=
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >='00:10:00'")[[1]], 2)
  # # Try <
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <'00:10:00'")[[1]], 1)
  # # Try <=
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <='00:10:00'")[[1]], 2)

  # # Try Is Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NULL")[[1]], 1)
  # # Try Is Not Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NOT NULL")[[1]], 3)

  # # Try And
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a='00:10:00' and b ='00:01:00'")[[1]], 0)
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a ='01:00:00' and b = '01:00:00' and c = '01:00:00'")[[1]], 1)
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a ='01:00:00' and b = '00:10:00' and c = '01:00:00'")[[1]], 0)
  # # Try Or
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a = '01:00:00' or b ='00:01:00'")[[1]], 2)

  duckdb::duckdb_unregister_arrow(con, "testarrow")
})

test_that("duckdb_register_arrow() performs selection pushdown timestamp type", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("CREATE TABLE test (a  TIMESTAMP, b TIMESTAMP, c TIMESTAMP)"))
  dbExecute(con, "INSERT INTO  test VALUES ('2008-01-01 00:00:01','2008-01-01 00:00:01','2008-01-01 00:00:01'),('2010-01-01 10:00:01','2010-01-01 10:00:01','2010-01-01 10:00:01'),('2020-03-01 10:00:01','2010-01-01 10:00:01','2020-03-01 10:00:01'),(NULL,NULL,NULL)")
  arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow = TRUE))
  duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

  # Try ==
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a ='2008-01-01 00:00:01'")[[1]], 1)
  # Try >
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >'2008-01-01 00:00:01'")[[1]], 2)
  # Try >=
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >='2010-01-01 10:00:01'")[[1]], 2)
  # Try <
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <'2010-01-01 10:00:01'")[[1]], 1)
  # Try <=
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <='2010-01-01 10:00:01'")[[1]], 2)

  # Try Is Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NULL")[[1]], 1)
  # Try Is Not Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NOT NULL")[[1]], 3)

  # Try And
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a='2010-01-01 10:00:01' and b ='2008-01-01 00:00:01'")[[1]], 0)
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a ='2020-03-01 10:00:01' and b = '2010-01-01 10:00:01' and c = '2020-03-01 10:00:01'")[[1]], 1)
  # Try Or
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a = '2020-03-01 10:00:01' or b ='2008-01-01 00:00:01'")[[1]], 2)

  duckdb::duckdb_unregister_arrow(con, "testarrow")
})

test_that("duckdb_register_arrow() performs selection pushdown timestamptz type", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("CREATE TABLE test (a  TIMESTAMPTZ, b TIMESTAMPTZ, c TIMESTAMPTZ)"))
  dbExecute(con, "INSERT INTO  test VALUES ('2008-01-01 00:00:01','2008-01-01 00:00:01','2008-01-01 00:00:01'),('2010-01-01 10:00:01','2010-01-01 10:00:01','2010-01-01 10:00:01'),('2020-03-01 10:00:01','2010-01-01 10:00:01','2020-03-01 10:00:01'),(NULL,NULL,NULL)")
  arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow = TRUE))
  duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

  # Try ==
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a ='2008-01-01 00:00:01'")[[1]], 1)
  # Try >
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >'2008-01-01 00:00:01'")[[1]], 2)
  # Try >=
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >='2010-01-01 10:00:01'")[[1]], 2)
  # Try <
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <'2010-01-01 10:00:01'")[[1]], 1)
  # Try <=
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <='2010-01-01 10:00:01'")[[1]], 2)

  # Try Is Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NULL")[[1]], 1)
  # Try Is Not Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NOT NULL")[[1]], 3)

  # Try And
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a='2010-01-01 10:00:01' and b ='2008-01-01 00:00:01'")[[1]], 0)
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a ='2020-03-01 10:00:01' and b = '2010-01-01 10:00:01' and c = '2020-03-01 10:00:01'")[[1]], 1)
  # Try Or
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a = '2020-03-01 10:00:01' or b ='2008-01-01 00:00:01'")[[1]], 2)

  duckdb::duckdb_unregister_arrow(con, "testarrow")
})

test_that("duckdb_register_arrow() performs selection pushdown date type", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbExecute(con, paste0("CREATE TABLE test (a  DATE, b DATE, c DATE)"))
  dbExecute(con, "INSERT INTO  test VALUES ('2000-01-01','2000-01-01','2000-01-01'),('2000-10-01','2000-10-01','2000-10-01'),('2010-01-01','2000-10-01','2010-01-01'),(NULL,NULL,NULL)")
  arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow = TRUE))
  duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

  # Try ==
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a ='2000-01-01'")[[1]], 1)
  # Try >
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >'2000-01-01'")[[1]], 2)
  # Try >=
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >='2000-10-01'")[[1]], 2)
  # Try <
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <'2000-10-01'")[[1]], 1)
  # Try <=
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <='2000-10-01'")[[1]], 2)

  # Try Is Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NULL")[[1]], 1)
  # Try Is Not Null
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NOT NULL")[[1]], 3)

  # Try And
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a='2000-10-01' and b ='2000-01-01'")[[1]], 0)
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a ='2010-01-01' and b = '2000-10-01' and c = '2010-01-01'")[[1]], 1)
  # Try Or
  expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a = '2010-01-01' or b ='2000-01-01'")[[1]], 2)

  duckdb::duckdb_unregister_arrow(con, "testarrow")
})

test_that("duckdb_register_arrow() under many threads", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  ds <- arrow::InMemoryDataset$create(mtcars)
  duckdb::duckdb_register_arrow(con, "mtcars_arrow", ds)
  dbExecute(con, "PRAGMA threads=32")
  dbExecute(con, "PRAGMA verify_parallelism")
  expect_error(dbGetQuery(con, "SELECT cyl, COUNT(mpg) FROM mtcars_arrow GROUP BY cyl"), NA)
  expect_error(dbGetQuery(con, "SELECT cyl, SUM(mpg) FROM mtcars_arrow GROUP BY cyl"), NA)
  expect_error(dbGetQuery(con, "SELECT cyl, AVG(mpg) FROM mtcars_arrow GROUP BY cyl"), NA)
})

test_that("we can unregister in finalizers yay", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))
  ds <- arrow::InMemoryDataset$create(mtcars)

  # Creates an environment that disconnects the database when it's GC'd
  duckdb_disconnector <- function(con, table_name) {
    # we need to force the name here
    table_name_forced <- force(table_name)
    reg.finalizer(environment(), function(...) {
      duckdb::duckdb_unregister_arrow(con, table_name_forced)
    })
    environment()
  }

  for (i in 1:100) {
    table_name <- paste0("mtcars_", i)
    duckdb::duckdb_register_arrow(con, table_name, ds)
    object_to_clean <- duckdb_disconnector(con, table_name)
  }
  object_to_clean <- NULL # otherwise we leak one
  # force a gc run, now they should all be gone
  gc()

  expect_equal(length(duckdb::duckdb_list_arrow(con)), 0)
})


test_that("we can list registered arrow tables", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))
  ds <- arrow::InMemoryDataset$create(mtcars)

  expect_equal(length(duckdb::duckdb_list_arrow(con)), 0)

  duckdb::duckdb_register_arrow(con, "t1", ds)
  duckdb::duckdb_register_arrow(con, "t2", ds)

  expect_equal(duckdb::duckdb_list_arrow(con), c("t1", "t2"))

  duckdb::duckdb_unregister_arrow(con, "t1")
  expect_equal(duckdb::duckdb_list_arrow(con), c("t2"))
  duckdb::duckdb_unregister_arrow(con, "t2")

  expect_equal(length(duckdb::duckdb_list_arrow(con)), 0)
})


test_that("duckdb can read arrow timestamps", {
  con <- DBI::dbConnect(duckdb::duckdb(), timezone_out = "UTC")
  on.exit(dbDisconnect(con, shutdown = TRUE))

  timestamp <- as.POSIXct("2022-01-30 11:59:29", tz = "UTC")

  for (unit in c("s", "ms", "us", "ns")) {
    tbl <- arrow::arrow_table(t = arrow::Array$create(timestamp, type = arrow::timestamp(unit)))
    duckdb::duckdb_register_arrow(con, "timestamps", tbl)

    if (unit == "ns") {
      # warning when precision loss
      expect_warning({ res <- dbGetQuery(con, "SELECT t FROM timestamps") })
    } else {
      expect_warning({ res <- dbGetQuery(con, "SELECT t FROM timestamps") }, regexp = NA)
    }
    expect_equal(res[[1]], as.POSIXct(as.character(timestamp), tz = "UTC"))

    res <- dbGetQuery(con, "SELECT year(t), month(t), day(t), hour(t), minute(t), second(t) FROM timestamps")

    expect_equal(res[[1]], 2022)
    expect_equal(res[[2]], 1)
    expect_equal(res[[3]], 30)
    expect_equal(res[[4]], 11)
    expect_equal(res[[5]], 59)
    expect_equal(res[[6]], 29)

    duckdb::duckdb_unregister_arrow(con, "timestamps")
  }
})

test_that("duckdb can read arrow timestamptz", {
  skip("ICU not loaded")
  con <- DBI::dbConnect(duckdb::duckdb(), timezone_out = "UTC")
  on.exit(dbDisconnect(con, shutdown = TRUE))

  timestamp <- as.POSIXct("2022-01-30 11:59:29")

  for (unit in c("s", "ms", "us", "ns")) {
    tbl <- arrow::arrow_table(t = arrow::Array$create(timestamp, type = arrow::timestamp(unit, "UTC")))
    duckdb::duckdb_register_arrow(con, "timestamps", tbl)

    if (unit == "ns") {
      # warning when precision loss
      expect_warning({ res <- dbGetQuery(con, "SELECT t FROM timestamps") })
    } else {
      expect_warning({ res <- dbGetQuery(con, "SELECT t FROM timestamps") }, regexp = NA)
    }
    expect_equal(res[[1]], as.POSIXct(as.character(timestamp), tz = "UTC"))

    res <- dbGetQuery(con, "SELECT year(t), month(t), day(t), hour(t), minute(t), second(t) FROM timestamps")

    expect_equal(res[[1]], 2022)
    expect_equal(res[[2]], 1)
    expect_equal(res[[3]], 30)
    expect_equal(res[[4]], 11)
    expect_equal(res[[5]], 59)
    expect_equal(res[[6]], 29)

    duckdb::duckdb_unregister_arrow(con, "timestamps")
  }
})


