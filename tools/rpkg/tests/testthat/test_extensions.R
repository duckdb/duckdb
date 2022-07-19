#skip_on_cran()

library("DBI")
library("testthat")

load_extension <- function(extension_name) {
  con <- dbConnect(duckdb::duckdb(config=list("allow_unsigned_extensions"="true")))
  matches <- list.files(pattern='*.duckdb_extension', recursive=FALSE, full.names=TRUE)
  matches <- c(matches, list.files(path='/tmp/duckdb_extensions', pattern='*.duckdb_extension', recursive=TRUE, full.names=TRUE))
  matches <- c(matches, list.files(path=system.file(package = 'duckdb'), pattern='*.duckdb_extension', recursive=TRUE, full.names=TRUE))

  require_variable <- Sys.getenv("DUCKDB_R_TEST_EXTENSION_REQUIRED")

  if (require_variable == "1") {
    expect_true(length(matches) > 0, "DUCKDB_R_TEST_EXTENSION_REQUIRED was set, but no extensions were found")
  } else {
    if (length(matches) == 0) {
        warning("No extensions found for testing!")
    }
  }
  found <- FALSE
  for (extension in matches){
    # Test loading the extension
    # HTTPFS extension specific test
    if (grepl(extension_name, extension, fixed = TRUE)) {
        query <- paste("LOAD '", extension, "';", sep = "")
        dbExecute(con, query)
        found <-TRUE
    }
  }
  if (found != TRUE){
  warning(paste("Required extension ", extension_name, "not found"))
  }
  return (con)
}

test_that("extensions can be loaded", {
    skip_if(Sys.getenv("DUCKDB_R_TEST_EXTENSION_REQUIRED") != "1", "DUCKDB_R_TEST_EXTENSION_REQUIRED not set, hence not testing extensions")
    con <- load_extension("httpfs.duckdb_extension")
    on.exit(dbDisconnect(con, shutdown = TRUE))
    df0 <- data.frame(
         id = c(1, 2, 3),
         first_name = c("Amanda", "Albert", "Evelyn"),
         last_name = c("Jordan", "Freeman", "Morgan"),
         stringsAsFactors = FALSE
       )
    res <- dbGetQuery(con, "SELECT id, first_name, last_name FROM PARQUET_SCAN('https://raw.githubusercontent.com/cwida/duckdb/master/data/parquet-testing/userdata1.parquet') LIMIT 3;")
    expect_equal(df0, res)
})

test_that("substrait extension test", {
  skip_if(Sys.getenv("DUCKDB_R_TEST_EXTENSION_REQUIRED") != "1", "DUCKDB_R_TEST_EXTENSION_REQUIRED not set, hence not testing extensions")
  con <- load_extension("substrait.duckdb_extension")
  on.exit(dbDisconnect(con, shutdown = TRUE))
  dbExecute(con, "CREATE TABLE integers (i INTEGER)")
  dbExecute(con, "INSERT INTO integers VALUES (42)")
  plan <- duckdb::duckdb_get_substrait(con, "select * from integers limit 5")
  result <- duckdb::duckdb_prepare_substrait(con, plan)
  df <- dbFetch(result)
  expect_equal(df$i, 42L)

  result_arrow <- duckdb::duckdb_prepare_substrait(con, plan, TRUE)
  df2 <- as.data.frame(duckdb::duckdb_fetch_arrow(result_arrow))
  expect_equal(df2$i, 42L)
})

