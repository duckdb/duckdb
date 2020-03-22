library(testthat)

test_convert <- function(con, type, val) {
	q <- dbSendQuery(con, sprintf("SELECT CAST(? AS %s) a", type))
	dbBind(q, list(val))
	res1 <- dbFetch(q)
	dbBind(q, list(NA))
	res2 <- dbFetch(q)
	dbClearResult(q)
	expect_equal(res1[[1]][1], val)
	expect_true(is.na(res2[[1]][1]))

}

test_that("dbBind() works as expected for all types", {
	library("DBI")

	con <- dbConnect(duckdb::duckdb())
	test_convert(con, "BOOLEAN", TRUE)
	test_convert(con, "BOOLEAN", FALSE)

	test_convert(con, "INTEGER", 42L)
	test_convert(con, "INTEGER", 42)

	test_convert(con, "DOUBLE", 42L)
	test_convert(con, "DOUBLE", 42.2)

	test_convert(con, "STRING", "Hello, World")
	
	test_convert(con, "DATE", as.Date("2019-11-26"))

	test_convert(con, "TIMESTAMP", as.POSIXct("2019-11-26 21:11Z", "UTC"))

	#test_convert(con, "STRING", as.factor("Hello, World"))
})
