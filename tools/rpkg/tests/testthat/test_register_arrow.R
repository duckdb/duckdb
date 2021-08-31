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
  expect_error(dbGetQuery(con, "SELECT first_name, last_name FROM myreader LIMIT 100"))

#   # cant register something non-arrow
#   expect_error(duckdb_register_arrow(con, "asdf", data.frame()))

  dbDisconnect(con, shutdown = T)
})

test_that("duckdb_register_arrow() works with datasets", {
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
  
})


test_that("duckdb_register_arrow() performs projection pushdown", {
    skip_if_not_installed("arrow", "4.0.1")

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
    skip_if_not_installed("arrow", "4.0.1")

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


numeric_operators <- function(data_type) {
    skip_if_not_installed("arrow", "4.0.1")
    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE TABLE test (a ",data_type,", b ",data_type,", c ",data_type,")"))
    dbExecute(con, "INSERT INTO  test VALUES (1,1,1),(10,10,10),(100,10,100),(NULL,NULL,NULL)")
    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE),return_table=TRUE)
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



test_that("duckdb_register_arrow() performs selection pushdown numeric types", {
    skip_if_not_installed("arrow", "4.0.1")

    numeric_types <- c('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT',
    'FLOAT', 'DOUBLE')

    for (data_type in numeric_types)
        numeric_operators(data_type)

})

# ArrowNotImplementedError: Function equal has no kernel matching input types (array[decimal128(4, 1)], scalar[decimal128(4, 1)])

test_that("duckdb_register_arrow() performs selection pushdown hugeint type", {
    skip_if_not_installed("arrow", "4.0.1")
    numeric_types <- c('HUGEINT')

    for (data_type in numeric_types)
        expect_error(numeric_operators(data_type))
        

})

# ArrowNotImplementedError: Function equal has no kernel matching input types (array[decimal128(4, 1)], scalar[decimal128(4, 1)])

test_that("duckdb_register_arrow() performs selection pushdown decimal types", {
    skip_if_not_installed("arrow", "4.0.1")
    numeric_types <- c('DECIMAL(4,1)','DECIMAL(9,1)','DECIMAL(18,4)','DECIMAL(30,12)')
    for (data_type in numeric_types)
        expect_error(numeric_operators(data_type))

})

test_that("duckdb_register_arrow() performs selection pushdown varchar type", {
    skip_if_not_installed("arrow", "4.0.1")
    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE TABLE test (a  VARCHAR, b VARCHAR, c VARCHAR)"))
    dbExecute(con, "INSERT INTO  test VALUES ('1','1','1'),('10','10','10'),('100','10','100'),(NULL,NULL,NULL)")
    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE),return_table=TRUE)
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
    dbDisconnect(con, shutdown = T)
})

test_that("duckdb_register_arrow() performs selection pushdown bool type", {
    skip_if_not_installed("arrow", "4.0.1")
    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE TABLE test (a  BOOL, b BOOL)"))
    dbExecute(con, "INSERT INTO  test VALUES (TRUE,TRUE),(TRUE,FALSE),(FALSE,TRUE),(NULL,NULL)")
    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE),return_table=TRUE)
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
    dbDisconnect(con, shutdown = T)
})

# NotImplemented: Function equal has no kernel matching input types (array[time64[us]], scalar[time32[s]])
test_that("duckdb_register_arrow() performs selection pushdown time type", {
    skip_if_not_installed("arrow", "4.0.1")

    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE TABLE test (a  TIME, b TIME, c TIME)"))
    dbExecute(con, "INSERT INTO  test VALUES ('00:01:00','00:01:00','00:01:00'),('00:10:00','00:10:00','00:10:00'),('01:00:00','00:10:00','01:00:00'),(NULL,NULL,NULL)")
    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE),return_table=TRUE)
    duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

    # Try ==
    expect_error(dbGetQuery(con, "SELECT count(*) from testarrow where a ='00:01:00'"))
    # # Try >
    # expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >'00:01:00'")[[1]], 2)
    # # Try >=
    # expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a >='00:10:00'")[[1]], 2)
    # # Try <
    # expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <'00:10:00'")[[1]], 1)
    # # Try <=
    # expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a <='00:10:00'")[[1]], 2)

    # # Try Is Null
    # expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NULL")[[1]], 1)
    # # Try Is Not Null
    # expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a IS NOT NULL")[[1]], 3)

    # # Try And
    # expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a='00:10:00' and b ='00:01:00'")[[1]], 0)
    # expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a ='01:00:00' and b = '00:10:00' and c = '01:00:00'")[[1]], 1)
    # # Try Or
    # expect_equal(dbGetQuery(con, "SELECT count(*) from testarrow where a = '01:00:00' or b ='00:01:00'")[[1]], 2)

    duckdb::duckdb_unregister_arrow(con, "testarrow")
    dbDisconnect(con, shutdown = T)

})

test_that("duckdb_register_arrow() performs selection pushdown timestamp type", {
    skip_if_not_installed("arrow", "4.0.1")

    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE TABLE test (a  TIMESTAMP, b TIMESTAMP, c TIMESTAMP)"))
    dbExecute(con, "INSERT INTO  test VALUES ('2008-01-01 00:00:01','2008-01-01 00:00:01','2008-01-01 00:00:01'),('2010-01-01 10:00:01','2010-01-01 10:00:01','2010-01-01 10:00:01'),('2020-03-01 10:00:01','2010-01-01 10:00:01','2020-03-01 10:00:01'),(NULL,NULL,NULL)")
    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE),return_table=TRUE)
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
    dbDisconnect(con, shutdown = T)
})

test_that("duckdb_register_arrow() performs selection pushdown date type", {
    skip_if_not_installed("arrow", "4.0.1")
    
    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE TABLE test (a  DATE, b DATE, c DATE)"))
    dbExecute(con, "INSERT INTO  test VALUES ('2000-01-01','2000-01-01','2000-01-01'),('2000-10-01','2000-10-01','2000-10-01'),('2010-01-01','2000-10-01','2010-01-01'),(NULL,NULL,NULL)")
    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE),return_table=TRUE)
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
    dbDisconnect(con, shutdown = T)
})

test_that("duckdb_register_arrow() under many threads", {
    con <- DBI::dbConnect(duckdb::duckdb())
    ds <- arrow::InMemoryDataset$create(mtcars)
    duckdb::duckdb_register_arrow(con, "mtcars_arrow", ds)
    DBI::dbExecute(con, "PRAGMA threads=32")
    DBI::dbExecute(con, "PRAGMA force_parallelism")
    expect_error(DBI::dbGetQuery(con, "SELECT cyl, COUNT(mpg) FROM mtcars_arrow GROUP BY cyl"),NA)

    expect_error(DBI::dbGetQuery(con, "SELECT cyl, SUM(mpg) FROM mtcars_arrow GROUP BY cyl"),NA)


    expect_error(DBI::dbGetQuery(con, "SELECT cyl, AVG(mpg) FROM mtcars_arrow GROUP BY cyl"),NA)

})


