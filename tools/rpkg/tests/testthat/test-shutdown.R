test_that("disconnect releases database file", {
  db_path <- withr::local_tempfile(fileext = ".duckdb")

  session_1 <- callr::r_session$new()
  withr::defer(session_1$kill())
  session_2 <- callr::r_session$new()
  withr::defer(session_2$kill())

  session_1$run(
    function(db_path) {
      .GlobalEnv$con <- DBI::dbConnect(duckdb::duckdb(), db_path)
      DBI::dbWriteTable(con, "test", data.frame(a = 1))
    },
    list(db_path = db_path)
  )

  expect_error(session_2$run(
    function(db_path) {
      .GlobalEnv$con <- DBI::dbConnect(duckdb::duckdb(), db_path)
    },
    list(db_path = db_path)
  ))

  session_1$run(function() {
    DBI::dbDisconnect(con, shutdown = TRUE)
  })

  session_2$run(
    function(db_path) {
      .GlobalEnv$con <- DBI::dbConnect(duckdb::duckdb(), db_path)
      DBI::dbDisconnect(con, shutdown = TRUE)
    },
    list(db_path = db_path)
  )
})
