library("DBI")
library("testthat")

test_that("Bug #2622, writing reserved column names", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))
  tname <- basename(tempfile("temp_"))
  tdata <- data.frame(id = 1:3, name = c("cuthbert", "dibble", "grubb"))

  dbWriteTable(con, tname, tdata, field.types = c(id = "INTEGER", name = "VARCHAR"))

  expect_equal(dbListTables(con), tname)
  expect_equal(dbReadTable(con, tname), tdata)
})
