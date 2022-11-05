test_that("one-level lists can be read", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  res <- dbGetQuery(con, "SELECT [] a")$a
  expect_equal(res, list(integer(0)))

  res <- dbGetQuery(con, "SELECT [42] a")$a
  expect_equal(res, list(42))

  res <- dbGetQuery(con, "SELECT [NULL] a")$a
  expect_true(is.na(res[[1]]))

  res <- dbGetQuery(con, "SELECT [42] a UNION ALL SELECT NULL")$a
  expect_true(is.null(res[[2]]))

  res <- dbGetQuery(con, "SELECT [42, 43] a")$a
  expect_equal(res, list(c(42, 43)))

  res <- dbGetQuery(con, "SELECT [42] a union all select [43]")$a
  expect_equal(res, list(42, 43))

  res <- dbGetQuery(con, "SELECT [42, 43, 44] a union all select [45, 46]")$a
  expect_equal(res, list(c(42, 43, 44), c(45, 46)))

  res <- dbGetQuery(con, "SELECT ['Hello', 'World'] a union all select ['There']")$a
  expect_equal(res, list(c("Hello", "World"), c("There")))
})
