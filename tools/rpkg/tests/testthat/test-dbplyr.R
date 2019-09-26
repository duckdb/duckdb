library("dbplyr")
library("testthat")

zipfile <- tempfile()
url <- "https://github.com/tidyverse/dbplyr/archive/v1.4.2.zip"
download.file(url, zipfile)
dbplyr_src <- tempdir()
unzip(zipfile, exdir=dbplyr_src)
tests <- Sys.glob(file.path(dbplyr_src, "dbplyr*", "tests"))
setwd(tests)

test_register_con("duckdb", duckdb::duckdb())
res <- test_check("dbplyr", stop_on_failure=TRUE, filter="(verb-joins|verb-mutate)", invert=T)
