test_that("factors can be round tripped", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  df0 <- data.frame(
    a = c(1, 2, 3),
    f = as.factor(c("a", "b", NA)),
    x = c("Hello", "World", "Etc"),
    stringsAsFactors = FALSE
  )

  duckdb::duckdb_register(con, "df0", df0)
  df1 <- dbReadTable(con, "df0")
  expect_equal(df0, df1)

  dbWriteTable(con, "df1", df0)
  df2 <- dbReadTable(con, "df1")
  expect_equal(df0, df2)
})


test_that("iris can be round-tripped", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  duckdb::duckdb_register(con, "iris", iris)
  df1 <- dbReadTable(con, "iris")
  expect_identical(iris, df1)

  dbWriteTable(con, "iris2", iris)
  df2 <- dbReadTable(con, "iris2")
  expect_identical(iris, df2)
})

test_that("non-utf things can be read", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  horrid_string <- iconv("Mühleisen", "utf8", "latin1")
  Encoding(horrid_string) <- "latin1"

  # both column name and factor level and plain string are latin1
  df <- data.frame(a = factor(horrid_string), b = horrid_string, stringsAsFactors = FALSE)
  names(df) <- c(horrid_string, "less_horrid")

  duckdb::duckdb_register(con, "df", df)
  df1 <- dbReadTable(con, "df")

  dbWriteTable(con, "df2", df)
  df2 <- dbReadTable(con, "df2")

  # fix the encoding again
  levels(df[[1]]) <- iconv(levels(df[[1]]), "latin1", "UTF8")
  names(df) <- iconv(names(df), "latin1", "UTF8")
  df$less_horrid <- iconv(df$less_horrid, "latin1", "UTF8")

  expect_identical(df, df1)
  expect_identical(df, df2)
})


test_that("single value factors round trip correctly, issue 2627", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  df1 <- data.frame(year = as.factor(rep("1998", 5)))
  dbWriteTable(con, "df", df1, field.types = c(year = "VARCHAR"))
  df2 <- dbReadTable(con, "df")
  df1$year <- as.character(df1$year)
  expect_identical(df1, df2)
})


test_that("huge-cardinality factors do not cause strange crashes, issue 3639", {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  set.seed(123)
  df <- data.frame(col1 = factor(sample(5000, 10^6, replace=TRUE)))
  duckdb_register(con, "df", df)
})
