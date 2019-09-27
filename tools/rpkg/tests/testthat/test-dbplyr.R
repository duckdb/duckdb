library("dbplyr")
library("testthat")

# pull dbplyr sources to harvest test cases
zipfile <- tempfile()
url <- "https://github.com/tidyverse/dbplyr/archive/v1.4.2.zip"
download.file(url, zipfile)
dbplyr_src <- tempdir()
unzip(zipfile, exdir=dbplyr_src)
tests <- Sys.glob(file.path(dbplyr_src, "dbplyr*", "tests"))
setwd(tests)


# TODO evil hack, remove when temp tables work
dbplyr_ns <- getNamespace("dbplyr")$.__S3MethodsTable__.
no_temp_copy_to <- function(con, table, values,
                            overwrite = FALSE, types = NULL, temporary = TRUE,
                            unique_indexes = NULL, indexes = NULL,
                            analyze = TRUE, ...) {

	dbplyr_ns[["db_copy_to.DBIConnection"]](con, table, values,
                            overwrite , types , temporary = FALSE,
                            unique_indexes , indexes ,
                            analyze, ...)
}
dbplyr_ns[["db_copy_to.duckdb_connection"]] <- no_temp_copy_to


test_register_con("duckdb", duckdb::duckdb())

# TODO fix excluded test cases
res <- test_check("dbplyr", stop_on_failure=TRUE, filter="(verb-joins|verb-mutate)", invert=T)
