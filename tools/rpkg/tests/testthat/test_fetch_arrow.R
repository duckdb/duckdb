library("testthat")
library("DBI")


test_that("duckdb_fetch_arrow() test table over vector size", {
    skip_on_os("windows")
    skip_if_not_installed("arrow", "4.0.1")

    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE table test as select range a from range(10000);"))
    dbExecute(con, "INSERT INTO  test VALUES(NULL);") 
    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE))
    duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

    expect_equal(dbGetQuery(con, "SELECT * from testarrow"), dbGetQuery(con, "SELECT * from test"))

    duckdb::duckdb_unregister_arrow(con, "testarrow")
    dbDisconnect(con, shutdown = T)
})

test_that("duckdb_fetch_arrow() empty table", {
    skip_on_os("windows")
    skip_if_not_installed("arrow", "4.0.1")

    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE TABLE test (a  INTEGER)"))

    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE))
    duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

    expect_equal(dbGetQuery(con, "SELECT * from testarrow"), dbGetQuery(con, "SELECT * from test"))

    duckdb::duckdb_unregister_arrow(con, "testarrow")
    dbDisconnect(con, shutdown = T)
})

test_that("duckdb_fetch_arrow() table with only nulls", {
    skip_on_os("windows")
    skip_if_not_installed("arrow", "4.0.1")

    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE TABLE test (a  INTEGER)"))

    dbExecute(con, "INSERT INTO  test VALUES(NULL);") 
    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE))
    duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

    expect_equal(dbGetQuery(con, "SELECT * from testarrow"), dbGetQuery(con, "SELECT * from test"))

    duckdb::duckdb_unregister_arrow(con, "testarrow")
    dbDisconnect(con, shutdown = T)
})

test_that("duckdb_fetch_arrow() table with prepared statement", {
    skip_on_os("windows")
    skip_if_not_installed("arrow", "4.0.1")

    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE TABLE test (a  INTEGER)"))
    dbExecute(con, paste0("PREPARE s1 AS INSERT INTO test VALUES ($1), ($2 / 2)"))
    for (value in 1:1500){
      dbExecute(con, sprintf("EXECUTE s1 (%d, %d);", value,value*2)) 
    }
    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE))
    duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

    expect_equal(dbGetQuery(con, "SELECT * from testarrow"), dbGetQuery(con, "SELECT * from test"))

    duckdb::duckdb_unregister_arrow(con, "testarrow")
    dbDisconnect(con, shutdown = T)
})

test_that("duckdb_fetch_arrow() streaming test", {
    skip_on_os("windows")
    skip_if_not_installed("arrow", "4.0.1")

    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE table t as select range a from range(3000);"))
    res <- dbSendQuery(con, "SELECT * FROM t", arrow=TRUE)
    cur_chunk <- duckdb::duckdb_fetch_arrow(res, stream=TRUE)
    expect_equal(1024,cur_chunk$column(0)$length())
    
    cur_chunk <- duckdb::duckdb_fetch_arrow(res, stream=TRUE)
    expect_equal(1024,cur_chunk$column(0)$length())
    
    cur_chunk <- duckdb::duckdb_fetch_arrow(res, stream=TRUE)
    expect_equal(952,cur_chunk$column(0)$length())

    cur_chunk <- duckdb::duckdb_fetch_arrow(res, stream=TRUE)
    expect_equal(0,cur_chunk$column(0)$length())

    dbDisconnect(con, shutdown = T)
})

test_that("duckdb_fetch_arrow() streaming test with vector_per_chunk parameter", {
    skip_on_os("windows")
    skip_if_not_installed("arrow", "4.0.1")

    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE table t as select range a from range(10000);"))
    res <- dbSendQuery(con, "SELECT * FROM t", arrow=TRUE)

    cur_chunk <- duckdb::duckdb_fetch_arrow(res, TRUE,2)
    expect_equal(2048,cur_chunk$column(0)$length())
    
    cur_chunk <- duckdb::duckdb_fetch_arrow(res, stream=TRUE)
    expect_equal(1024,cur_chunk$column(0)$length())
    
    cur_chunk <- duckdb::duckdb_fetch_arrow(res, stream=TRUE,vector_per_chunk=3)
    expect_equal(3072,cur_chunk$column(0)$length())

    cur_chunk <- duckdb::duckdb_fetch_arrow(res, stream=TRUE,vector_per_chunk=0)
    expect_equal(0,cur_chunk$column(0)$length())


    cur_chunk <- duckdb::duckdb_fetch_arrow(res, stream=TRUE,vector_per_chunk=1)
    expect_equal(1024,cur_chunk$column(0)$length())

    cur_chunk <- duckdb::duckdb_fetch_arrow(res, stream=TRUE,vector_per_chunk=100)
    expect_equal(2832,cur_chunk$column(0)$length())

    dbDisconnect(con, shutdown = T)
})


test_that("duckdb_fetch_arrow() streaming test with  negative vector_per_chunk parameter", {
    skip_on_os("windows")
    skip_if_not_installed("arrow", "4.0.1")

    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE table t as select range a from range(5);"))
    res <- dbSendQuery(con, "SELECT * FROM t", arrow=TRUE)

    expect_error(duckdb::duckdb_fetch_arrow(res, TRUE,-1))
    cur_chunk <- duckdb::duckdb_fetch_arrow(res, TRUE)
    expect_equal(5,cur_chunk$column(0)$length())
    dbDisconnect(con, shutdown = T)
})


