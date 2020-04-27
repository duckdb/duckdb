library("testthat")

# the easy part
test_check("duckdb")

dbplyr_tests <- function() {
	if (!identical(Sys.getenv("NOT_CRAN"), "true")) {
		return()
	}
	# the hacky part
	library("dbplyr")

	# pull dbplyr sources to harvest test cases
	zipfile <- tempfile()
	url <- "https://github.com/tidyverse/dbplyr/archive/v1.4.2.zip"
	download.file(url, zipfile)
	dbplyr_src <- tempdir()
	unzip(zipfile, exdir=dbplyr_src)
	tests <- Sys.glob(file.path(dbplyr_src, "dbplyr*", "tests"))
	setwd(tests)

	options(duckdb.debug=T)
	test_register_src("duckdb", duckdb::src_duckdb())

	# TODO fix excluded test cases
	test_check("dbplyr", stop_on_failure=TRUE,, invert=T, filter="(verb-joins|verb-mutate)")
}
# dbplyr_tests()
