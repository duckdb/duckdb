library(testthat)

test_convert <- function(con, type, val) {
	val_comp <- val
	if (class(val) == "factor") {
		val_comp <- as.character(val)
	}
	q <- dbSendQuery(con, sprintf("SELECT CAST(? AS %s) a", type))
	dbBind(q, list(val))
	res1 <- dbFetch(q)
	dbBind(q, list(NA))
	res2 <- dbFetch(q)
	dbClearResult(q)
	expect_equal(res1[[1]][1], val_comp)
	expect_true(is.na(res2[[1]][1]))

	dbExecute(con, "DROP TABLE IF EXISTS bind_test")
	dbExecute(con, sprintf("CREATE TEMPORARY TABLE bind_test(i INTEGER, a %s)", type))
	q <- dbSendStatement(con, sprintf("INSERT INTO bind_test VALUES ($1, $2)", type))
	dbBind(q, list(1, val))
	dbBind(q, list(2, NA))
	dbClearResult(q)
	res3 <- dbGetQuery(con, "SELECT a FROM bind_test ORDER BY i")
	expect_equal(res3[[1]][1], val_comp)
	expect_true(is.na(res3[[1]][2]))
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

	test_convert(con, "STRING", as.factor("Hello, World"))
})
