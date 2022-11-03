skip_on_cran()
local_edition(3)

test_that("empty statement gives an error", {
  con <- DBI::dbConnect(duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  expect_snapshot_error(DBI::dbGetQuery(con, "; ;   ; -- SELECT 1;"))
})

test_that("multiple statements can be used in one call", {
  con <- DBI::dbConnect(duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  query <- paste(
    "CREATE TABLE integers(i integer);",
    "insert into integers select * from range(10);",
    "select * from integers;",
    sep = "\n"
  )
  expect_identical(DBI::dbGetQuery(con, query), data.frame(i = 0:9))
  expect_snapshot(DBI::dbGetQuery(con, paste("DROP TABLE IF EXISTS integers;", query)))
})

test_that("statements can be splitted apart correctly", {
  con <- DBI::dbConnect(duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  expect_snapshot(DBI::dbGetQuery(con, a <- paste(
    "--Multistatement testing; testing",
    "/*  test;   ",
    "--test;",
    ";test */",
    "create table temp_test as ",
    "select",
    "'testing_temp;' as temp_col",
    ";",
    "select * from temp_test;",
    sep = "\n"
  )))
})

test_that("export/import database works", {
  export_location <- file.path(tempdir(), "duckdb_test_export")
  if (!file.exists(export_location)) dir.create(export_location)

  con <- DBI::dbConnect(duckdb())
  DBI::dbExecute(con, "CREATE TABLE integers(i integer)")
  DBI::dbExecute(con, "insert into integers select * from range(10)")
  DBI::dbExecute(con, "CREATE TABLE integers2(i INTEGER)")
  DBI::dbExecute(con, "INSERT INTO integers2 VALUES (1), (5), (7), (1928)")
  DBI::dbExecute(con, paste0("EXPORT DATABASE '", export_location, "'"))
  DBI::dbDisconnect(con, shutdown = TRUE)

  con <- DBI::dbConnect(duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  DBI::dbExecute(con, paste0("IMPORT DATABASE '", export_location, "'"))
  if (file.exists(export_location)) unlink(export_location, recursive = TRUE)

  expect_identical(DBI::dbGetQuery(con, "select * from integers"), data.frame(i = 0:9))
  expect_identical(DBI::dbGetQuery(con, "select * from integers2"), data.frame(i = c(1L, 5L, 7L, 1928L)))
})
