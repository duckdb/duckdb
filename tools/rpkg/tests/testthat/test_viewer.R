test_that("rs_list_object_types", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  object_types <- rs_list_object_types(con)
  expect_true(length(object_types) == 1)

  dbExecute(con, "CREATE VIEW foo as SELECT 42")

  object_types <- rs_list_object_types(con)
  expect_true(length(object_types$schema$contains) == 2)
})

test_that("rs_list_objects", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  objects <- rs_list_objects(con)
  expect_equal(nrow(objects), 0)

  dbExecute(con, "CREATE TABLE a (j integer)")
  dbExecute(con, "CREATE VIEW b as SELECT 42")

  expect_equal(rs_list_objects(con), data.frame(name = c("a", "b"), type = c("table", "view"), stringsAsFactors = FALSE))

  dbExecute(con, "CREATE schema fuu ;")
  dbExecute(con, "CREATE TABLE fuu.x (j integer)")

  expect_equal(rs_list_objects(con, schema = "fuu"), data.frame(name = c("x"), type = c("table"), stringsAsFactors = FALSE))
})

test_that("rs_list_columns", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  objects <- rs_list_objects(con)
  expect_equal(nrow(objects), 0)

  dbExecute(con, "CREATE TABLE t (a integer, b string, c timestamp)")

  cmp <- data.frame(name = c("a", "b", "c"), field.type = c("INTEGER", "VARCHAR", "TIMESTAMP"), stringsAsFactors = FALSE)

  expect_equal(rs_list_columns(con, "t"), cmp)
  expect_equal(rs_list_columns(con, "t", schema = "main"), cmp)

  dbExecute(con, "CREATE schema fuu ;")
  dbExecute(con, "CREATE TABLE fuu.t (x integer, y string, z timestamp)")

  cmp <- data.frame(name = c("x", "y", "z"), field.type = c("INTEGER", "VARCHAR", "TIMESTAMP"), stringsAsFactors = FALSE)
  expect_equal(rs_list_columns(con, "t", schema = "fuu"), cmp)
})

test_that("rs_viewer", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbWriteTable(con, "mtcars", mtcars)

  row.names(mtcars) <- seq(1, nrow(mtcars))
  expect_equal(head(mtcars, 5), rs_preview(con, 5, table = "mtcars"))
})

test_that("rs_actions", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  rs_actions(con)
  expect_true(TRUE)
})

test_that("mock observer hooray", {
  called_connection_opened <- FALSE
  called_connection_closed <- FALSE
  called_connection_updated <- FALSE

  mock <- list(connectionOpened = function(host, ...) {
    called_connection_opened <<- TRUE
  }, connectionClosed = function(...) {
    called_connection_closed <<- TRUE
  }, connectionUpdated = function(...) {
    called_connection_updated <<- TRUE
  })
  options(connectionObserver = mock, duckdb.force_rstudio_connection_pane = TRUE)
  con <- dbConnect(duckdb())
  expect_true(called_connection_opened)

  dbWriteTable(con, "mtcars", mtcars)
  expect_true(called_connection_updated)
  called_connection_updated <- FALSE
  dbRemoveTable(con, "mtcars")
  expect_true(called_connection_updated)
  dbDisconnect(con, shutdown = TRUE)
  expect_true(called_connection_closed)

  options(connectionObserver = NULL, duckdb.force_rstudio_connection_pane = FALSE)
})
