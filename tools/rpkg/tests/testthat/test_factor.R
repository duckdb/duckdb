library("DBI")
library("testthat")

test_that("factors can be round tripped", {
  con <- dbConnect(duckdb::duckdb())

  df0 <- data.frame(f=as.factor(c('a', 'b', NA)))
  duckdb::duckdb_register(con, "df0", df0)
  df1 <- dbReadTable(con, "df0")
  expect_equal(df0, df1)

  dbWriteTable(con, "df1", df0)
  df2 <- dbReadTable(con, "df1")
  # This becomes a string because we cannot do a CREATE TABLE (a ENUM)
  expect_equal(as.character(df0$f), df2$f)
})
