test_that("unnest() is consistent with data frames", {
  local_edition(3)

  skip_if_not_installed("tidyr")
  skip_if_not_installed("dbplyr")

  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  data <- data.frame(groups = 1:2, num = 1:6, char = letters[1:6])
  tbl <- dplyr::copy_to(con, data)
  data_nested <- tidyr::nest(data, data = -groups)
  tbl_nested <- dplyr::compute(tidyr::nest(tbl, data = -groups))

  expect_snapshot({
    dbplyr::sql_render(tidyr::unnest(tbl_nested, data))
  })

  expect_equal(
    untibble(tidyr::unnest(data_nested, data)),
    untibble(dplyr::collect(tidyr::unnest(tbl_nested, data)))
  )
})

test_that("unpack() is consistent with data frames", {
  local_edition(3)

  skip_if_not_installed("tidyr")
  skip_if_not_installed("dbplyr")

  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  data <- data.frame(groups = 1:2, num = 1:6, char = letters[1:6])
  tbl <- dplyr::copy_to(con, data)
  data_packed <- tidyr::pack(data, data = -groups)
  tbl_packed <- dplyr::compute(pack.tbl_duckdb_connection(tbl, data = -groups))

  expect_snapshot({
    dbplyr::sql_render(unpack.tbl_duckdb_connection(tbl_packed, data))
  })

  expect_equal(
    untibble(tidyr::unpack(data_packed, data)),
    untibble(dplyr::collect(unpack.tbl_duckdb_connection(tbl_packed, data)))
  )
})
