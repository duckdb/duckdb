library("testthat")
library("DBI")

test_that("duckdb_register_arrow() works", {
  skip_on_os("windows")
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
  expect_error(dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 100"))

#   # cant register something non-arrow
#   expect_error(duckdb_register_arrow(con, "asdf", data.frame()))

  dbDisconnect(con, shutdown = T)
})

test_that("duckdb_register_arrow() works with datasets", {
  for (i in 1:10) {
    skip_on_os("windows")
    skip_if_not_installed("arrow", "4.0.1")
    con <- dbConnect(duckdb::duckdb())

    # Registering a dataset + aggregation
    ds <- arrow::open_dataset("userdata1.parquet")
    duckdb::duckdb_register_arrow(con, "mydatasetreader", ds)
    res1 <- dbGetQuery(con, "SELECT count(*) FROM mydatasetreader")
    res2 <- dbGetQuery(con, "SELECT count(*) FROM parquet_scan('userdata1.parquet')")
    expect_true(identical(res1, res2))
    # we can read with > 3 cores
    dbExecute(con, "PRAGMA threads=4")
    res3 <- dbGetQuery(con, "SELECT count(*) FROM mydatasetreader")
    expect_true(identical(res2, res3))
    duckdb::duckdb_unregister_arrow(con, "mydatasetreader")

    dbDisconnect(con, shutdown = T)
  }
})


test_that("duckdb_register_arrow() performs projection pushdown", {

    con <- dbConnect(duckdb::duckdb())

    # Registering a dataset + aggregation
    ds <- arrow::open_dataset("userdata1.parquet")
    duckdb::duckdb_register_arrow(con, "mydatasetreader", ds)

    res1 <- dbGetQuery(con, "SELECT last_name, salary, first_name FROM mydatasetreader")
    res2 <- dbGetQuery(con, "SELECT last_name, salary, first_name FROM parquet_scan('userdata1.parquet')")
    expect_true(identical(res1, res2))
    # we can read with > 3 cores
    dbExecute(con, "PRAGMA threads=4")
    res3 <- dbGetQuery(con, "SELECT last_name, salary, first_name FROM mydatasetreader")
    expect_true(identical(res2, res3))
    duckdb::duckdb_unregister_arrow(con, "mydatasetreader")

    dbDisconnect(con, shutdown = T)
})

test_that("duckdb_register_arrow() performs selection pushdown", {

    con <- dbConnect(duckdb::duckdb())

    # Registering a dataset + aggregation
    ds <- arrow::open_dataset("userdata1.parquet")
    duckdb::duckdb_register_arrow(con, "mydatasetreader", ds)

    res1 <- dbGetQuery(con, "SELECT last_name, first_name FROM mydatasetreader where salary > 130000")
    res2 <- dbGetQuery(con, "SELECT last_name, first_name FROM parquet_scan('userdata1.parquet')  where salary > 130000")
    expect_true(identical(res1, res2))
    # we can read with > 3 cores
    dbExecute(con, "PRAGMA threads=4")
    res3 <- dbGetQuery(con, "SELECT last_name, first_name FROM mydatasetreader where salary > 130000")
    expect_true(identical(res2, res3))
    duckdb::duckdb_unregister_arrow(con, "mydatasetreader")

    dbDisconnect(con, shutdown = T)
})


library("arrow")
numeric_operators <- function(data_type) {
    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE TABLE test (a ",data_type,", b ",data_type,", c ",data_type,")"))
    dbExecute(con, "INSERT INTO  test VALUES (1,1,1),(10,10,10),(100,10,100),(NULL,NULL,NULL)")
    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE))
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
    dbDisconnect(con, shutdown = T)
}



test_that("duckdb_register_arrow() performs selection pushdown may types", {

    numeric_types <- c('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT',
    'FLOAT', 'DOUBLE')

    for (data_type in numeric_types)
        numeric_operators(data_type)

})



