test_that("dbFetch() can fetch RETURNING statements (#3875)", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  dbCreateTable(con, "x", list(a = "int"))

  expect_silent(out <-dbGetQuery(con, "INSERT INTO x VALUES (1) RETURNING (a)"))
  expect_equal(out, data.frame(a = 1L))
})
