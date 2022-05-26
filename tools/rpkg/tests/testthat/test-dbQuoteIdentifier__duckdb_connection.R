test_that("only quotes where needed", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  expect_equal(
    dbQuoteIdentifier(con, c("x y", "select", "SELECT", "x")),
    SQL(c('"x y"', '"select"', '"SELECT"', "x"))
  )
})

test_that("preserves expected DBI behaviour", {
  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  expect_equal(dbQuoteIdentifier(con, SQL("SELECT")), SQL("SELECT"))
  expect_equal(dbQuoteIdentifier(con, character()), SQL(character()))
  expect_equal(dbQuoteIdentifier(con, c(a = "a")), SQL("a", names = "a"))

  expect_equal(
    dbQuoteIdentifier(con, Id(schema = "a", table = "b")),
    SQL("a.b")
  )
  expect_equal(
    dbQuoteIdentifier(con, Id(schema = "SELECT", table = "b")),
    SQL('"SELECT".b')
  )
})
