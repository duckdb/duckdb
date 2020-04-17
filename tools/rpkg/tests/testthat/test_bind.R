library("testthat")
library("DBI")

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
	q <- dbSendStatement(con, "INSERT INTO bind_test VALUES ($1, $2)")
	dbBind(q, list(1, val))
	dbBind(q, list(2, NA))
	dbClearResult(q)
	res3 <- dbGetQuery(con, "SELECT a FROM bind_test ORDER BY i")
	dbExecute(con, "DROP TABLE bind_test")

	expect_equal(res3[[1]][1], val_comp)
	expect_true(is.na(res3[[1]][2]))
}

test_that("dbBind() works as expected for all types", {
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
	
	dbDisconnect(con, shutdown=T)

})

test_that("dbBind() is called from dbGetQuery and dbExecute", {
	con <- dbConnect(duckdb::duckdb())

	res <- dbGetQuery(con, "SELECT CAST (? AS INTEGER), CAST(? AS STRING)", 42, "Hello")

	expect_equal(res[[1]][1], 42L)
	expect_equal(res[[2]][1], "Hello")

	res <- dbGetQuery(con, "SELECT CAST (? AS INTEGER), CAST(? AS STRING)", list(42, "Hello"))

	expect_equal(res[[1]][1], 42L)
	expect_equal(res[[2]][1], "Hello")


	q <- dbSendQuery(con, "SELECT CAST (? AS INTEGER), CAST(? AS STRING)", 42, "Hello")
	# already have a result

	res <- dbFetch(q)

	expect_equal(res[[1]][1], 42L)
	expect_equal(res[[2]][1], "Hello")


	# now bind again
	dbBind(q, list(43, "Holla"))

	res <- dbFetch(q)

	expect_equal(res[[1]][1], 43L)
	expect_equal(res[[2]][1], "Holla")


	dbClearResult(q)

	dbDisconnect(con, shutdown=T)

})

test_that("various error cases for dbBind()", {
	# testthat::skip("eek")
	con <- dbConnect(duckdb::duckdb())

	q <- dbSendQuery(con, "SELECT CAST (? AS INTEGER)")

	expect_error(dbFetch(q))

	expect_error(dbBind(q, list()))
	expect_error(dbBind(q, list(1, 2)))
	expect_error(dbBind(q, list("asdf", "asdf")))

	expect_error(dbBind(q))

	expect_error(dbBind(q, 1))
	expect_error(dbBind(q, 1, 2))
	expect_error(dbBind(q, "asdf"))
	expect_error(dbBind(q, "asdf", "asdf"))

	dbClearResult(q)

	expect_error(dbGetQuery(con, "SELECT CAST (? AS INTEGER)", 1, 2))
	expect_error(dbGetQuery(con, "SELECT CAST (? AS INTEGER)", "asdf"))
	expect_error(dbGetQuery(con, "SELECT CAST (? AS INTEGER)", "asdf", "asdf"))
	expect_error(dbGetQuery(con, "SELECT CAST (? AS INTEGER)"))
	expect_error(dbGetQuery(con, "SELECT CAST (? AS INTEGER)", list()))
	expect_error(dbGetQuery(con, "SELECT CAST (? AS INTEGER)", list(1, 2)))
	expect_error(dbGetQuery(con, "SELECT CAST (? AS INTEGER)", list("asdf")))
	expect_error(dbGetQuery(con, "SELECT CAST (? AS INTEGER)", list("asdf", "asdf")))

	q <- dbSendQuery(con, "SELECT CAST (42 AS INTEGER)")

	res <- dbFetch(q)
	expect_equal(res[[1]][1], 42L)

	expect_error(dbBind(q, list()))
	expect_error(dbBind(q, list(1)))
	expect_error(dbBind(q, list("asdf")))

	expect_error(dbBind(q))
	expect_error(dbBind(q, 1))
	expect_error(dbBind(q, "asdf"))

	dbClearResult(q)

	expect_error(dbGetQuery(con, "SELECT CAST (42 AS INTEGER)", 1))
	expect_error(dbGetQuery(con, "SELECT CAST (42 AS INTEGER)", 1, 2))
	expect_error(dbGetQuery(con, "SELECT CAST (42 AS INTEGER)", "asdf"))
	expect_error(dbGetQuery(con, "SELECT CAST (42 AS INTEGER)", "asdf", "asdf"))
	expect_error(dbGetQuery(con, "SELECT CAST (42 AS INTEGER)", list(1)))
	expect_error(dbGetQuery(con, "SELECT CAST (42 AS INTEGER)", list(1, 2)))
	expect_error(dbGetQuery(con, "SELECT CAST (42 AS INTEGER)", list("asdf")))
	expect_error(dbGetQuery(con, "SELECT CAST (42 AS INTEGER)", list("asdf", "asdf")))


	dbDisconnect(con, shutdown=T)
})
