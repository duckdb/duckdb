test_that("structs can be read", {
  skip_if_not_installed("vctrs")

  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  res <- dbGetQuery(con, "SELECT {'x': 100, 'y': 'hello', 'z': 3.14} AS s")
  expect_equal(res, vctrs::data_frame(
    s = vctrs::data_frame(x = 100L, y = "hello", z = 3.14)
  ))

  res <- dbGetQuery(con, "SELECT 1 AS n, {'x': 100, 'y': 'hello', 'z': 3.14} AS s")
  expect_equal(res, vctrs::data_frame(
    n = 1L,
    s = vctrs::data_frame(x = 100L, y = "hello", z = 3.14)
  ))

  res <- dbGetQuery(con, "values (100, {'x': 100}), (200, {'x': 200}), (300, NULL)")
  expect_equal(res, vctrs::data_frame(
    col0 = c(100L, 200L, 300L),
    col1 = vctrs::data_frame(x = c(100L, 200L, NA))
  ))

  res <- dbGetQuery(con, "values ('a', {'x': 100, 'y': {'a': 1, 'b': 2}}), ('b', {'x': 200, y: NULL}), ('c', NULL)")
  expect_equal(res, vctrs::data_frame(
    col0 = c("a", "b", "c"),
    col1 = vctrs::data_frame(
      x = c(100L, 200L, NA),
      y = vctrs::data_frame(a = c(1L, NA, NA), b = c(2L, NA, NA))
    )
  ))

  res <- dbGetQuery(con, "select 100 AS other, [{'x': 1, 'y': 'a'}, {'x': 2, 'y': 'b'}] AS s")
  expect_equal(res, vctrs::data_frame(
    other = 100L,
    s = list(
      vctrs::data_frame(x = c(1L, 2L), y = c("a", "b"))
    )
  ))

  res <- dbGetQuery(con, "values ([{'x': 1, 'y': 'a'}, {'x': 2, 'y': 'b'}]), ([]), ([{'x': 1, 'y': 'a'}])")
  expect_equal(res, vctrs::data_frame(
    col0 = list(
      vctrs::data_frame(x = c(1L, 2L), y = c("a", "b")),
      vctrs::data_frame(x = integer(0), y = character(0)),
      vctrs::data_frame(x = 1L, y = "a")
    )
  ))
})

test_that("structs give the same results via Arrow", {
  skip_on_cran()
  skip_if_not_installed("vctrs")
  skip_if_not_installed("tibble")
  skip_if_not_installed("arrow")

  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  res <- dbGetQuery(con, "SELECT {'x': 100, 'y': 'hello', 'z': 3.14} AS s", arrow = TRUE)
  expect_equal(res, tibble::tibble(
    s = tibble::tibble(x = 100L, y = "hello", z = 3.14)
  ))

  res <- dbGetQuery(con, "SELECT 1 AS n, {'x': 100, 'y': 'hello', 'z': 3.14} AS s", arrow = TRUE)
  expect_equal(res, tibble::tibble(
    n = 1L,
    s = tibble::tibble(x = 100L, y = "hello", z = 3.14)
  ))

  res <- dbGetQuery(con, "values (100, {'x': 100}), (200, {'x': 200}), (300, NULL)", arrow = TRUE)
  expect_equal(res, tibble::tibble(
    col0 = c(100L, 200L, 300L),
    col1 = tibble::tibble(x = c(100L, 200L, NA))
  ))

  res <- dbGetQuery(con, "values ('a', {'x': 100, 'y': {'a': 1, 'b': 2}}), ('b', {'x': 200, y: NULL}), ('c', NULL)", arrow = TRUE)
  expect_equal(res, tibble::tibble(
    col0 = c("a", "b", "c"),
    col1 = tibble::tibble(
      x = c(100L, 200L, NA),
      y = tibble::tibble(a = c(1L, NA, NA), b = c(2L, NA, NA))
    )
  ))

  res <- dbGetQuery(con, "select 100 AS other, [{'x': 1, 'y': 'a'}, {'x': 2, 'y': 'b'}] AS s", arrow = TRUE)
  expect_equal(res, tibble::tibble(
    other = 100L,
    s = vctrs::new_list_of(
      list(
        tibble::tibble(x = c(1L, 2L), y = c("a", "b"))
      ),
      ptype = tibble::tibble(x = integer(), y = character()),
      class = "arrow_list"
    )
  ))

  res <- dbGetQuery(con, "values ([{'x': 1, 'y': 'a'}, {'x': 2, 'y': 'b'}]), ([]), ([{'x': 1, 'y': 'a'}])", arrow = TRUE)
  expect_equal(res, tibble::tibble(
    col0 = vctrs::new_list_of(
      list(
        tibble::tibble(x = c(1L, 2L), y = c("a", "b")),
        tibble::tibble(x = integer(0), y = character(0)),
        tibble::tibble(x = 1L, y = "a")
      ),
      ptype = tibble::tibble(x = integer(), y = character()),
      class = "arrow_list"
    )
  ))
})

test_that("nested lists of atomic values can be written", {
  skip_if_not_installed("vctrs")

  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  df <- vctrs::data_frame(a = 1:3, b = list(4:6, 2:3, 1L))
  dbWriteTable(con, "df", df)
  expect_equal(dbReadTable(con, "df"), df)

  duckdb_register(con, "df_reg", df)
  expect_equal(dbReadTable(con, "df_reg"), df)

  df2 <- vctrs::data_frame(a = 1:2, b = list(4:6, letters[2:3]))
  expect_error(dbWriteTable(con, "df2", df2), "register")
  expect_error(duckdb_register(con, "df2_reg", df2), "register")
})

test_that("nested and packed columns work in full", {
  skip_if_not_installed("vctrs")

  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE))

  df <- vctrs::data_frame(
    a = vctrs::data_frame(
      x = 1:5,
      y = as.numeric(6:10),
      z = vctrs::data_frame(
        k = c(TRUE, FALSE, NA, TRUE, FALSE),
        l = letters[1:5]
      )
    ),
    b = list(
      vctrs::data_frame(
        u = structure(as.numeric(19577:19578), class = "Date"),
        v = structure(19577:19578, class = "Date"),
        w = structure(1691507820, class = c("POSIXct", "POSIXt"), tzone = "UTC") + 0:1
      )
    ),
    c = list(
      vctrs::data_frame(
        # TIME_MINUTES, TIME_MINUTES_INTEGER, TIME_HOURS, ... etc.: Loss ok
        d = structure(as.numeric(13:16), class = "difftime", units = "secs"),
        e = vctrs::data_frame(
          u = structure(17:20, class = "difftime", units = "secs"),
          v = 5:8,
          w = as.list(9:12)
        )
      )
    ),
    f = list(
      vctrs::data_frame(
        g = list(as.raw(13), as.raw(14:15), as.raw(16:18), as.raw(19:22)),
        h = vctrs::data_frame(u = 1:4, v = 5:8, w = list(vctrs::data_frame(s = 9:10)))
      )
    ),
    i = "plain old"
  )
  dbWriteTable(con, "df", df)
  expect_equal(dbReadTable(con, "df"), df)

  duckdb_register(con, "df_reg", df)
  expect_equal(dbReadTable(con, "df_reg"), df)
})
