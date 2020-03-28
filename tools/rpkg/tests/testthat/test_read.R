library("testthat")
library("DBI")

test_that("read_csv_duckdb() works as expected", {
	con <- dbConnect(duckdb::duckdb())
	
	tf <- tempfile()

	# default case
	write.csv(iris, tf, row.names = FALSE)
	read_csv_duckdb(con, tf, "iris")
	res <- dbReadTable(con, "iris")
	res$Species <- as.factor(res$Species)
	expect_true(identical(res, iris))

	# table exists
	read_csv_duckdb(con, tf, "iris")
	count <- dbGetQuery(con, "SELECT COUNT(*) FROM iris")[1][1]
	expect_true(identical(as.integer(count), as.integer(nrow(iris)*2)))
	dbRemoveTable(con, "iris")


	# different separator
	write.table(iris, tf, row.names = FALSE, sep=" ")
	read_csv_duckdb(con, tf, "iris", delim=" ")
	res <- dbReadTable(con, "iris")
	res$Species <- as.factor(res$Species)
	expect_true(identical(res, iris))
	dbRemoveTable(con, "iris")

	write.table(iris, tf, row.names = FALSE, sep=" ")
	read_csv_duckdb(con, tf, "iris", sep=" ")
	res <- dbReadTable(con, "iris")
	res$Species <- as.factor(res$Species)
	expect_true(identical(res, iris))
	dbRemoveTable(con, "iris")

	# no header
	write.table(iris, tf, row.names = FALSE, sep=",", col.names=FALSE)
	read_csv_duckdb(con, tf, "iris", header=FALSE)
	res <- dbReadTable(con, "iris")
	names(res) <- names(iris)
	res$Species <- as.factor(res$Species)
	expect_true(identical(res, iris))
	dbRemoveTable(con, "iris")

	# lowercase header
	write.csv(iris, tf, row.names = FALSE)
	read_csv_duckdb(con, tf, "iris", lower.case.names=T)
	res <- dbReadTable(con, "iris")
	res$species <- as.factor(res$species)
	iris_lc <- iris
	names(iris_lc) <- tolower(names(iris))
	expect_true(identical(res, iris_lc))
	dbRemoveTable(con, "iris")

	# nulls
	iris_na <- iris
	iris_na[[2]][42] <- NA

	write.csv(iris_na, tf, row.names = FALSE, na="")
	read_csv_duckdb(con, tf, "iris")
	res <- dbReadTable(con, "iris")
	res$Species <- as.factor(res$Species)
	expect_true(identical(res, iris_na))
	dbRemoveTable(con, "iris")


	write.csv(iris_na, tf, row.names = FALSE, na="NULL")
	read_csv_duckdb(con, tf, "iris", na.strings="NULL")
	res <- dbReadTable(con, "iris")
	res$Species <- as.factor(res$Species)
	expect_true(identical(res, iris_na))
	dbRemoveTable(con, "iris")


	# strange table name
	write.csv(iris, tf, row.names = FALSE)
	read_csv_duckdb(con, tf, "ir Is")
	res <- dbReadTable(con, "ir Is")
	res$Species <- as.factor(res$Species)
	expect_true(identical(res, iris))
	dbRemoveTable(con, "ir Is")


	# specified column names
	colnames <- paste0("c", 1:5)
	write.csv(iris, tf, row.names = FALSE)
	read_csv_duckdb(con, tf, "iris", col.names=colnames)
	res <- dbReadTable(con, "iris")
	res$c5 <- as.factor(res$c5)
	iris_c <- iris
	names(iris_c) <- colnames
	expect_true(identical(res, iris_c))
	dbRemoveTable(con, "iris")


	# multiple files
	tf2 <- tempfile()
	write.csv(iris, tf2, row.names = FALSE)
	read_csv_duckdb(con, c(tf, tf2), "iris")
	count <- dbGetQuery(con, "SELECT COUNT(*) FROM iris")[1][1]
	expect_true(identical(as.integer(count), as.integer(nrow(iris)*2)))
	dbRemoveTable(con, "iris")

	dbDisconnect(con, shutdown=T)

})
