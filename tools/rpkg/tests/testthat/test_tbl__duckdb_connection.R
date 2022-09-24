skip_on_cran()
`%>%` <- dplyr::`%>%`

test_that("Parquet files can be registered with dplyr::tbl()", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  tab0 <- dplyr::tbl(con, "data/userdata1.parquet")
  expect_true(inherits(tab0, "tbl_duckdb_connection"))
  expect_true(tab0 %>% dplyr::count() %>% dplyr::collect() == 1000)

  tab1 <- dplyr::tbl(con, "read_parquet(['data/userdata1.parquet'])")
  expect_true(inherits(tab1, "tbl_duckdb_connection"))
  expect_true(tab1 %>% dplyr::count() %>% dplyr::collect() == 1000)

  tab2 <- dplyr::tbl(con, "'data/userdata1.parquet'")
  expect_true(inherits(tab2, "tbl_duckdb_connection"))
  expect_true(tab2 %>% dplyr::count() %>% dplyr::collect() == 1000)

  tab3 <- dplyr::tbl(con, "parquet_scan(['data/userdata1.parquet'])")
  expect_true(inherits(tab3, "tbl_duckdb_connection"))
  expect_true(tab3 %>% dplyr::count() %>% dplyr::collect() == 1000)
})


test_that("Object cache can be enabled for parquet files with dplyr::tbl()", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  DBI::dbExecute(con, "SET enable_object_cache=False;")
  tab1 <- dplyr::tbl(con, "data/userdata1.parquet", cache = TRUE)
  expect_true(DBI::dbGetQuery(con, "SELECT value FROM duckdb_settings() WHERE name='enable_object_cache';") == "true")

  DBI::dbExecute(con, "SET enable_object_cache=False;")
  tab2 <- dplyr::tbl(con, "'data/userdata1.parquet'", cache = FALSE)
  expect_true(DBI::dbGetQuery(con, "SELECT value FROM duckdb_settings() WHERE name='enable_object_cache';") == "false")
})


test_that("CSV files can be registered with dplyr::tbl()", {
  path <- file.path(tempdir(), "duckdbtest.csv")
  write.csv(iris, file = path)
  on.exit(unlink(path))

  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  tab1 <- dplyr::tbl(con, path)
  expect_true(inherits(tab1, "tbl_duckdb_connection"))
  expect_true(tab1 %>% dplyr::count() %>% dplyr::collect() == 150)

  tab2 <- dplyr::tbl(con, paste0("read_csv_auto('", path, "')"))
  expect_true(inherits(tab2, "tbl_duckdb_connection"))
  expect_true(tab2 %>% dplyr::count() %>% dplyr::collect() == 150)
})

test_that("Other replacement scans or functions can be registered with dplyr::tbl()", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  obj <- dplyr::tbl(con, "duckdb_keywords()")
  expect_true(inherits(obj, "tbl_duckdb_connection"))
  expect_true(obj %>% dplyr::filter(keyword_name == "all") %>% dplyr::count() %>% dplyr::collect() == 1)
})

rm(`%>%`)