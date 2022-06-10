test_that("nest() is consistent with data frames", {
  local_edition(3)

  skip_if_not_installed("tidyr")
  skip_if_not_installed("dbplyr")

  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  data <- data.frame(groups = 1:2, num = 1:6, char = letters[1:6])
  tbl <- dplyr::copy_to(con, data)

  expect_snapshot({
    dbplyr::sql_render(tidyr::nest(tbl, data = -groups))
  })

  expect_equal(
    untibble(tidyr::nest(data, data = -groups)),
    untibble(dplyr::collect(tidyr::nest(tbl, data = -groups)))
  )
})

test_that("pack() is consistent with data frames", {
  local_edition(3)

  skip_if_not_installed("tidyr")
  skip_if_not_installed("dbplyr")

  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  data <- data.frame(groups = 1:2, num = 1:6, char = letters[1:6])
  tbl <- dplyr::copy_to(con, data)

  expect_snapshot({
    dbplyr::sql_render(pack.tbl_duckdb_connection(tbl, data = -groups))
  })

  expect_equal(
    untibble(tidyr::pack(data, data = -groups)),
    untibble(dplyr::collect(pack.tbl_duckdb_connection(tbl, data = -groups)))
  )
})
