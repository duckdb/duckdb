test_that("dbWriteTable can write tables with keyword column names", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  # NB: name is a reserved word, will need to be escaped as part of writing operation
  sample_data <- data.frame(id = 1:3, name = c("cuthbert", "dibble", "grubb"))
  dbWriteTable(con, "sample_data", sample_data, field.types = c(id = "INTEGER", name = "VARCHAR"))

  # Can read the data we wrote back again
  expect_identical(dbReadTable(con, "sample_data"), sample_data)
})
