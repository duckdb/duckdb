library("DBI")
library("testthat")

test_that("factors can be round tripped", {
  con <- dbConnect(duckdb::duckdb())

  df0 <- data.frame(
    a=c(1,2, 3), 
    f=as.factor(c('a', 'b', NA)), 
    x=c('Hello', 'World', 'Etc'), 
    stringsAsFactors=FALSE)
  
  duckdb::duckdb_register(con, "df0", df0)
  df1 <- dbReadTable(con, "df0")
  expect_equal(df0, df1)

  dbWriteTable(con, "df1", df0)
  df2 <- dbReadTable(con, "df1")
  expect_equal(df0, df2)
})
