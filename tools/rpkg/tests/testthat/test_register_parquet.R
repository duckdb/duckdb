test_that("duckdb_register_parquet() works", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  duckdb::duckdb_register_parquet(con, "testview", "data/userdata1.parquet")
  rs1 <- DBI::dbGetQuery(con, "SELECT first_name, last_name FROM testview LIMIT 10")
  rs2 <- DBI::dbGetQuery(con, "SELECT first_name, last_name FROM parquet_scan('data/userdata1.parquet') LIMIT 10")
  expect_true(identical(rs1, rs2))
  # we can re-read
  rs3 <- DBI::dbGetQuery(con, "SELECT first_name, last_name FROM testview LIMIT 10")
  expect_true(identical(rs2, rs3))
  duckdb::duckdb_unregister_parquet(con, "testview")
  # cant read after unregister
  expect_error(DBI::dbGetQuery(con, "SELECT first_name, last_name FROM testview LIMIT 10"))
})

test_that("binary_as_string flag can be used with duckdb_register_parquet", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  duckdb::duckdb_register_parquet(con, name = "bintrue", path = "data/binary_string.parquet", binary_as_string = TRUE)
  duckdb::duckdb_register_parquet(con, name = "binfalse", path = "data/binary_string.parquet")
  rs1 <- DBI::dbGetQuery(con, "SELECT * FROM bintrue LIMIT 1;")
  rs2 <- DBI::dbGetQuery(con, "SELECT * FROM binfalse LIMIT 1;")

  expect_true(rs1 == rawToChar(unlist(rs2)))
})

test_that("duckdb_register_parquet gives errors on invalid arguments", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))


  expect_error(duckdb::duckdb_register_parquet(1, name = "data", path = "data/userdata1.parquet"))
  expect_error(duckdb::duckdb_register_parquet(con, name = "", path = "data/userdata1.parquet"))
  expect_error(duckdb::duckdb_register_parquet(con, name = "data", path = "data/userdata999.parquet"))
  expect_error(duckdb::duckdb_register_parquet(con, name = "data"))

  duckdb::duckdb_register_parquet(con, name = "data1", path = "data/userdata1.parquet")
  expect_error(duckdb::duckdb_unregister_parquet(1, "data1"))
  expect_error(duckdb::duckdb_unregister_parquet(con, ""))
})

test_that("more than one view can be dropped at the same query with duckdb_unregister_parquet", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  duckdb::duckdb_register_parquet(con, name = "bintrue", path = "data/binary_string.parquet", binary_as_string = TRUE)
  duckdb::duckdb_register_parquet(con, name = "binfalse", path = "data/binary_string.parquet")
  rs1 <- length(DBI::dbListTables(con))

  duckdb::duckdb_unregister_parquet(con, c("bintrue", "binfalse"))
  rs2 <- length(DBI::dbListTables(con))

  expect_true(rs2 == rs1 - 2)
})
