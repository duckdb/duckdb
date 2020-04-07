library("testthat")
library("DBI")


test_that("duckdb_register() works", {
	con <- dbConnect(duckdb::duckdb())
	
	# most basic case
	duckdb::duckdb_register(con, "my_df1", iris)
	res <- dbReadTable(con, "my_df1")
	res$Species <- as.factor(res$Species)
	expect_true(identical(res, iris))
	duckdb::duckdb_unregister(con, "my_df1")

	duckdb::duckdb_register(con, "my_df2", mtcars)
	res <- dbReadTable(con, "my_df2")
	row.names(res) <- row.names(mtcars)
	expect_true(identical(res, mtcars))
	duckdb::duckdb_unregister(con, "my_df2")

	duckdb::duckdb_register(con, "my_df1", mtcars)
	res <- dbReadTable(con, "my_df1")
	row.names(res) <- row.names(mtcars)
	expect_true(identical(res, mtcars))

 	# do not need unregister, can simply overwrite
	duckdb::duckdb_register(con, "my_df1", iris)
	res <- dbReadTable(con, "my_df1")
	res$Species <- as.factor(res$Species)
	expect_true(identical(res, iris))

	duckdb::duckdb_unregister(con, "my_df1")
	duckdb::duckdb_unregister(con, "my_df2")
	duckdb::duckdb_unregister(con, "xxx")

	# this needs to be empty now
	expect_true(length(attributes(con@conn_ref)) == 0)

	dbDisconnect(con, shutdown=T)
})


test_that("various error cases for duckdb_register()", {
	con <- dbConnect(duckdb::duckdb())

	duckdb::duckdb_register(con, "my_df1", iris)
	duckdb::duckdb_unregister(con, "my_df1")
	expect_error(dbReadTable(con, "my_df1"))

	expect_error(duckdb::duckdb_register(1, "my_df1", iris))
	expect_error(duckdb::duckdb_register(con, "", iris))
	expect_error(duckdb::duckdb_unregister(1, "my_df1"))
	expect_error(duckdb::duckdb_unregister(con, ""))
	dbDisconnect(con, shutdown=T)

	expect_error(duckdb::duckdb_unregister(con, "my_df1"))

})
