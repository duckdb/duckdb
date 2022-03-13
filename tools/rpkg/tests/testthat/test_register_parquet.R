test_that("duckdb_register_parquet() works", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  duckdb::duckdb_register_parquet(con, "testview", "data/userdata1.parquet")
  rs1 <- DBI::dbGetQuery(con, "SELECT first_name, last_name FROM testview LIMIT 10")
  rs2 <- DBI::dbGetQuery(con, "SELECT first_name, last_name FROM parquet_scan('data/userdata1.parquet') LIMIT 10")
  expect_true(identical(rs1, rs2))
  # we can re-read
  rs3 <- DBI::dbGetQuery(con, "SELECT first_name, last_name FROM testview LIMIT 10")
  expect_true(identical(rs2, rs3))
  duckdb::duckdb_unregister_parquet(con, "testview")
  # cant read after unregister
  expect_error(DBI::dbGetQuery(con, "SELECT first_name, last_name FROM testview LIMIT 10"))
})

test_that("binary_as_string flag can be used with duckdb_register_parquet", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  duckdb::duckdb_register_parquet(con, name = "bintrue", path = "data/binary_string.parquet", binary_as_string = TRUE)
  duckdb::duckdb_register_parquet(con, name = "binfalse", path = "data/binary_string.parquet")
  rs1 <- DBI::dbGetQuery(con, "SELECT * FROM bintrue LIMIT 1;")
  rs2 <- DBI::dbGetQuery(con, "SELECT * FROM binfalse LIMIT 1;")

  expect_true(rs1 == rawToChar(unlist(rs2)))
})

test_that("duckdb_register_parquet gives errors on invalid arguments", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))


  expect_error(duckdb::duckdb_register_parquet(1, name = "data", path = "data/userdata1.parquet"))
  expect_error(duckdb::duckdb_register_parquet(con, name = "", path = "data/userdata1.parquet"))
  expect_error(duckdb::duckdb_register_parquet(con, name = "data", path = "data/userdata999.parquet"))
  expect_error(duckdb::duckdb_register_parquet(con, name = "data"))

  duckdb::duckdb_register_parquet(con, name = "data1", path = "data/userdata1.parquet")
  expect_error(duckdb::duckdb_unregister_parquet(1, "data1"))
  expect_error(duckdb::duckdb_unregister_parquet(con, ""))
})

test_that("more than one view can be dropped at the same query with duckdb_unregister_parquet", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  duckdb::duckdb_register_parquet(con, name = "bintrue", path = "data/binary_string.parquet", binary_as_string = TRUE)
  duckdb::duckdb_register_parquet(con, name = "binfalse", path = "data/binary_string.parquet")
  rs1 <- length(DBI::dbListTables(con))

  duckdb::duckdb_unregister_parquet(con, c("bintrue", "binfalse"))
  rs2 <- length(DBI::dbListTables(con))

  expect_true(rs2 == rs1 - 2)
})

test_that("parameters replace and temporary work as expected", {

  # Set up the test environment
  filename <- "data/userdata1.parquet"
  tmpdir <- file.path(tempdir(), "rstest")
  if (!file.exists(tmpdir)) dir.create(tmpdir)
  unlink(paste0(tmpdir, "/*.*"))
  tmp <- paste0(tmpdir, "/file", 1:2, ".parquet")
  on.exit(unlink(tmpdir, recursive = TRUE))

  # Create two parquet files
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbExecute(con, paste0("COPY (SELECT * FROM parquet_scan('", filename, "') LIMIT 30) TO '", tmp[1], "' (FORMAT 'parquet');"))
  DBI::dbExecute(con, paste0("COPY (SELECT * EXCLUDE comments FROM parquet_scan('", filename, "') LIMIT 10) TO '", tmp[2], "' (FORMAT 'parquet');"))
  DBI::dbDisconnect(con, shutdown = TRUE)

  # Check that replacement scan registering works
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  duckdb::duckdb_register_parquet(con, "testview", tmp[1])
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 30)

  # Check that overwriting without replace=TRUE gives an error
  expect_error(duckdb::duckdb_register_parquet(con, "testview", tmp[2]))

  # Check that overwriting works with replace=TRUE
  duckdb::duckdb_register_parquet(con, "testview", tmp[2], replace = TRUE)
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 10)

  # Check that replace does not happen too early
  expect_error(duckdb::duckdb_register_parquet(con, "testview", "", replace = TRUE))
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 10)

  # Check that a temporary view can be created (with warning) even if a view with same name exists
  expect_warning(duckdb::duckdb_register_parquet(con, "testview", tmp[1], temporary = TRUE))
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 30)
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM main.testview;") == 10)
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) as n FROM information_schema.tables") == 2)

  # Check that unregistering clears the temporary view
  duckdb::duckdb_unregister_parquet(con, "testview")
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 10)
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) as n FROM information_schema.tables") == 1)

  # Check that schema with temporary table gives an error
  expect_error(duckdb::duckdb_register_parquet(con, "main.testview", tmp[1], temporary = TRUE))

  # Check that schema gives an error in any case
  expect_error(duckdb::duckdb_register_parquet(con, "temp.testview", tmp[1]))
})





test_that("parameters replace and temporary work as expected", {

  # Set up the test environment
  filename <- "data/userdata1.parquet"
  tmpdir <- file.path(tempdir(), "rstest")
  dbpath <- file.path(tmpdir, "test.duckdb")
  if (!file.exists(tmpdir)) dir.create(tmpdir)
  unlink(paste0(tmpdir, "/*.*"))
  tmp <- paste0(tmpdir, "/file", 1:2, ".parquet")
  on.exit(unlink(tmpdir, recursive = TRUE))

  # Create two parquet files
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbExecute(con, paste0("COPY (SELECT * FROM parquet_scan('", filename, "') LIMIT 30) TO '", tmp[1], "' (FORMAT 'parquet');"))
  DBI::dbExecute(con, paste0("COPY (SELECT * EXCLUDE comments FROM parquet_scan('", filename, "') LIMIT 10) TO '", tmp[2], "' (FORMAT 'parquet');"))
  DBI::dbDisconnect(con, shutdown = TRUE)

  # Check that temporary view works as expected in file-storage
  con <- DBI::dbConnect(duckdb::duckdb(dbdir = dbpath))
  duckdb::duckdb_register_parquet(con, "testview", tmp[1], replace = TRUE, temporary = TRUE)
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 30)
  duckdb::duckdb_register_parquet(con, "testview", tmp[2], replace = TRUE, temporary = TRUE)
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 10)
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) as n FROM information_schema.tables") == 1)
  DBI::dbDisconnect(con, shutdown = TRUE)

  # Check that temporary view is not permanently saved in file-storage
  con <- DBI::dbConnect(duckdb::duckdb(dbdir = dbpath))
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) as n FROM information_schema.tables") == 0)
  duckdb::duckdb_register_parquet(con, "testview", tmp[1])
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 30)
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) as n FROM information_schema.tables") == 1)
  DBI::dbDisconnect(con, shutdown = TRUE)

  # Check that non-temporary view is not permanently saved in file-storage
  con <- DBI::dbConnect(duckdb::duckdb(dbdir = dbpath, read_only = TRUE))
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) as n FROM information_schema.tables") == 1)
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 30)

  # Check whether permanent view can be dropped in read-only connection
  expect_error(duckdb::duckdb_unregister_parquet(con, "testview"))
  expect_error(duckdb::duckdb_unregister_parquet(con, "testview", if_exists = TRUE))
  expect_error(duckdb::duckdb_unregister_parquet(con, "testview2"))
  duckdb::duckdb_unregister_parquet(con, "testview2", if_exists = TRUE)
  expect_warning(duckdb::duckdb_register_parquet(con, "testview", tmp[2], temporary = TRUE))
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 10)
  duckdb::duckdb_unregister_parquet(con, "testview")
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 30)
  expect_error(duckdb::duckdb_register_parquet(con, "testview", tmp[2], replace = TRUE))

  # Check what happens if parquet file disappears
  unlink(tmp[1])
  expect_error(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;"))

  file.copy(tmp[2], tmp[1])
  expect_error(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;"))

  DBI::dbDisconnect(con, shutdown = TRUE)
})


test_that("Parquet files tolerate manipulation", {
  # Set up the test environment
  filename <- "data/userdata1.parquet"
  tmpdir <- file.path(tempdir(), "rstest")
  dbpath <- file.path(tmpdir, "test2.duckdb")
  if (!file.exists(tmpdir)) dir.create(tmpdir)
  unlink(paste0(tmpdir, "/*.*"))
  tmp <- paste0(tmpdir, "/file", 1:2, ".parquet")
  on.exit(unlink(tmpdir, recursive = TRUE))
  tmpwild <- paste0(tmpdir, "/file*.parquet")

  # Create two parquet files
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbExecute(con, paste0("COPY (SELECT * EXCLUDE comments FROM parquet_scan('", filename, "') LIMIT 30) TO '", tmp[1], "' (FORMAT 'parquet');"))
  DBI::dbExecute(con, paste0("COPY (SELECT * EXCLUDE comments FROM parquet_scan('", filename, "') LIMIT 10) TO '", tmp[2], "' (FORMAT 'parquet');"))
  DBI::dbDisconnect(con, shutdown = TRUE)

  # Check that wildcard identified Parquet-files work as expected in file-storage
  con <- DBI::dbConnect(duckdb::duckdb(dbdir = dbpath))
  duckdb::duckdb_register_parquet(con, "testview", tmpwild, replace = TRUE)
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 40)

  # Check what happens if parquet file disappears
  unlink(tmp[1])
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 10)

  DBI::dbExecute(con, paste0("COPY (SELECT * EXCLUDE comments FROM parquet_scan('", filename, "') LIMIT 15) TO '", tmp[1], "' (FORMAT 'parquet');"))
  expect_true(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM testview;") == 25)

  DBI::dbDisconnect(con, shutdown = TRUE)
})
