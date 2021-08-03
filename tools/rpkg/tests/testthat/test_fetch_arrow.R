library("testthat")
library("DBI")

skip_on_os("windows")
skip_if_not_installed("arrow", "4.0.1")
# Skip if parquet is not a capabiltiy as an indicator that Arrow is fully installed.
full_arrow <- "parquet" %in% names(arrow::arrow_info()$capabilities) && arrow::arrow_info()$capabilities["parquet"]
skip_if_not(full_arrow, message = "The installed Arrow is not fully featured, skipping Arrow integration tests")

test_that("duckdb_fetch_arrow() test table over vector size", {
    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE TABLE test (a  INTEGER)"))
    for (value in 1:10000){
      dbExecute(con, sprintf("INSERT INTO  test VALUES(%d);", value))
    }
    dbExecute(con, "INSERT INTO  test VALUES(NULL);")
    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE))
    duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

    expect_equal(dbGetQuery(con, "SELECT * from testarrow"), dbGetQuery(con, "SELECT * from test"))

    duckdb::duckdb_unregister_arrow(con, "testarrow")
    dbDisconnect(con, shutdown = T)
})

test_that("duckdb_fetch_arrow() empty table", {
    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE TABLE test (a  INTEGER)"))

    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE))
    duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

    expect_equal(dbGetQuery(con, "SELECT * from testarrow"), dbGetQuery(con, "SELECT * from test"))

    duckdb::duckdb_unregister_arrow(con, "testarrow")
    dbDisconnect(con, shutdown = T)
})

test_that("duckdb_fetch_arrow() table with only nulls", {
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
    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, paste0("CREATE TABLE test (a  INTEGER)"))
    dbExecute(con, paste0("PREPARE s1 AS INSERT INTO test VALUES ($1), ($2 / 2)"))
    for (value in 1:5000){
      dbExecute(con, sprintf("EXECUTE s1 (%d, %d);", value,value*2))
    }
    arrow_table <- duckdb::duckdb_fetch_arrow(dbSendQuery(con, "SELECT * FROM test", arrow=TRUE))
    duckdb::duckdb_register_arrow(con, "testarrow", arrow_table)

    expect_equal(dbGetQuery(con, "SELECT * from testarrow"), dbGetQuery(con, "SELECT * from test"))

    duckdb::duckdb_unregister_arrow(con, "testarrow")
    dbDisconnect(con, shutdown = T)
})