skip_on_cran()
skip_if_no_R4 <- function() {
  if (R.Version()$major < 4) {
    skip("R 4.0.0 or newer not available for testing")
  }
}

test_that("Parquet files can be registered with dplyr::tbl()", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  tab1 <- dplyr::tbl(con, "data/userdata1.parquet")
  expect_true(any(grepl("duckdb_connection", class(tab1))))
  expect_true(tab1 |> dplyr::count() |> dplyr::collect() == 1000)

  tab2 <- dplyr::tbl(con, "'data/userdata1.parquet'")
  expect_true(any(grepl("duckdb_connection", class(tab2))))
  expect_true(tab2 |> dplyr::count() |> dplyr::collect() == 1000)

  tab3 <- dplyr::tbl(con, "parquet_scan(['data/userdata1.parquet'])")
  expect_true(any(grepl("duckdb_connection", class(tab3))))
  expect_true(tab3 |> dplyr::count() |> dplyr::collect() == 1000)
  
  tab4 <- dplyr::tbl(con, "read_parquet(['data/userdata1.parquet'])")
  expect_true(any(grepl("duckdb_connection", class(tab4))))
  expect_true(tab4 |> dplyr::count() |> dplyr::collect() == 1000)
})


test_that("Object cache can be enabled for parquet files with dplyr::tbl()", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  DBI::dbExecute(con, "SET enable_object_cache=False;")
  tab1 <- dplyr::tbl(con, "data/userdata1.parquet", cache=TRUE)
  expect_true(DBI::dbGetQuery(con, "SELECT value FROM duckdb_settings() WHERE name='enable_object_cache';") == "True")
  
  DBI::dbExecute(con, "SET enable_object_cache=False;")
  tab2 <- dplyr::tbl(con, "data/userdata1.parquet", cache = FALSE)
  expect_true(DBI::dbGetQuery(con, "SELECT value FROM duckdb_settings() WHERE name='enable_object_cache';") == "False")
})


test_that("CSV files can be registered with dplyr::tbl()", {
  path <- file.path(tempdir(), "duckdbtest.csv")
  write.csv(iris, file = path)
  on.exit(unlink(path))

  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  tab1 <- dplyr::tbl(con, path)
  expect_true(any(grepl("duckdb_connection", class(tab1))))
  expect_true(tab1 |> dplyr::count() |> dplyr::collect() == 150)

  tab2 <- dplyr::tbl(con, paste0("read_csv_auto('", path, "')"))
  expect_true(any(grepl("duckdb_connection", class(tab2))))
  expect_true(tab2 |> dplyr::count() |> dplyr::collect() == 150)
})
