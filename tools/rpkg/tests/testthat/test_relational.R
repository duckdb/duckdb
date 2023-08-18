# Run this file with testthat::test_local(filter = "^relational$")

con <- dbConnect(duckdb())
on.exit(dbDisconnect(con, shutdown = TRUE))

test_that("we can create a relation from a df", {
  rel <- rel_from_df(con, mtcars)
  expect_type(rel, "externalptr")
  expect_s3_class(rel, "duckdb_relation")
})

test_that("we won't crash when creating a relation from odd things", {
  # na seems to be fine, single col data frame with a single row with NA in it
  rel_from_df(con, NA)

  expect_error(rel_from_df(NULL, NULL))
  expect_error(rel_from_df(con, NULL))

  expect_true(TRUE)
})

test_that("we can round-trip a data frame", {
  expect_equivalent(mtcars, as.data.frame.duckdb_relation(rel_from_df(con, mtcars)))
  expect_equivalent(iris, as.data.frame.duckdb_relation(rel_from_df(con, iris)))
})


test_that("we can create various expressions and don't crash", {
  ref <- expr_reference("asdf")
  print(ref)
  expect_error(expr_reference(NA))
  #  expect_error(expr_reference(as.character(NA)))
  expect_error(expr_reference(""))
  expect_error(expr_reference(NULL))

  expr_constant(TRUE)
  expr_constant(FALSE)
  expr_constant(NA)
  expr_constant(42L)
  expr_constant(42)
  const <- expr_constant("asdf")
  print(const)

  expect_error(expr_constant(NULL))
  expect_error(expr_constant())


  expr_function("asdf", list())

  expect_error(expr_function("", list()))
  #  expect_error(expr_function(as.character(NA), list()))
  expect_error(expr_function(NULL, list()))
  expect_error(expr_function("asdf"))

  expect_true(TRUE)
})


# TODO should maybe be a different file, test_enum_strings.R

test_that("we can cast R strings to DuckDB strings", {
  chars <- c(letters, LETTERS)
  max_len <- 100
  n <- 100000

  # yay R one-liners
  gen_rand_string <- function(x, max_len) paste0(chars[sample(1:length(chars), runif(1) * max_len, replace = TRUE)], collapse = "")

  test_string_vec <- c(vapply(1:n, gen_rand_string, "character", max_len), NA, NA, NA, NA, NA, NA, NA, NA) # batman

  df <- data.frame(s = test_string_vec, stringsAsFactors = FALSE)
  expect_equivalent(df, as.data.frame.duckdb_relation(rel_from_df(con, df)))

  res <- rel_from_df(con, df) |> rel_sql("SELECT s::string FROM _")
  expect_equivalent(df, res)

  res <- rel_from_df(con, df) |> rel_sql("SELECT COUNT(*) c FROM _")
  expect_equal(nrow(df), res$c)

  # many rounds yay
  df2 <- df
  for (i in 1:10) {
    df2 <- as.data.frame.duckdb_relation(rel_from_df(con, df2))
    expect_equivalent(df, df2)
  }

  df2 <- df
  for (i in 1:10) {
    df2 <- as.data.frame(rel_from_df(con, df2) |> rel_sql("SELECT s::string s FROM _"))
    expect_equivalent(df, df2)
  }
})

test_that("the altrep-conversion for relations works", {
  iris$Species <- as.character(iris$Species)
  rel <- rel_from_df(con, iris)
  df <- rel_to_altrep(rel)
  expect_false(df_is_materialized(df))
  inspect_output <- capture.output(.Internal(inspect(df)))
  expect_true(any(grepl("DUCKDB_ALTREP_REL_VECTOR", inspect_output, fixed = TRUE)))
  expect_true(any(grepl("DUCKDB_ALTREP_REL_ROWNAMES", inspect_output, fixed = TRUE)))
  expect_false(df_is_materialized(df))
  dim(df)
  expect_true(df_is_materialized(df))
  expect_equal(iris, df)
})

test_that("the altrep-conversion for relations work for weirdo types", {
  test_df <- data.frame(col_date = as.Date("2019-11-26"), col_ts = as.POSIXct("2019-11-26 21:11Z", "UTC"), col_factor = factor(c("a")))
  rel <- rel_from_df(con, test_df)
  df <- rel_to_altrep(rel)
  expect_false(df_is_materialized(df))
  expect_equal(test_df, df)
})

test_that("we can get the relation object back from an altrep df", {
  iris$Species <- as.character(iris$Species)
  rel <- rel_from_df(con, iris)
  df <- rel_to_altrep(rel)
  rel2 <- rel_from_altrep_df(df)
  expect_true(TRUE)
})

test_that("rel_order() sorts NAs last", {
  test_df <- rel_from_df(con, data.frame(a = c(NA, 1:3)))

  orders <- list(expr_reference("a"))

  rel <- rel_order(test_df, orders)
  rel_df <- rel_to_altrep(rel)
  expect_false(df_is_materialized(rel_df))

  expected_result <- data.frame(a = c(1:3, NA))
  expect_equal(rel_df, expected_result)
})

test_that("Inner join returns all inner relations", {
  dbExecute(con, "CREATE OR REPLACE MACRO eq(a, b) AS a = b")
  left <- rel_from_df(con, data.frame(left_a = c(1, 2, 3), left_b = c(1, 1, 2)))
  right <- rel_from_df(con, data.frame(right_b = c(1, 3), right_c = c(4, 5)))
  cond <- list(expr_function("eq", list(expr_reference("left_b"), expr_reference("right_b"))))
  rel2 <- rel_join(left, right, cond, "inner")
  rel_df <- rel_to_altrep(rel2)
  dim(rel_df)
  expected_result <- data.frame(left_a = c(1, 2), left_b = c(1, 1), right_b = c(1, 1), right_c = c(4, 4))
  expect_equal(rel_df, expected_result)
})

test_that("Left join returns all left relations", {
  dbExecute(con, "CREATE OR REPLACE MACRO eq(a, b) AS a = b")
  left <- rel_from_df(con, data.frame(left_a = c(1, 2, 3), left_b = c(1, 1, 2)))
  right <- rel_from_df(con, data.frame(right_b = c(1)))
  cond <- list(expr_function("eq", list(expr_reference("left_b"), expr_reference("right_b"))))
  rel2 <- rel_join(left, right, cond, "left")
  rel_df <- rel_to_altrep(rel2)
  dim(rel_df)
  expected_result <- data.frame(left_a = c(1, 2, 3), left_b = c(1, 1, 2), right_b = c(1, 1, NA))
  expect_equal(rel_df, expected_result)
})

test_that("Right join returns all right relations", {
  dbExecute(con, "CREATE OR REPLACE MACRO eq(a, b) AS a = b")
  left <- rel_from_df(con, data.frame(left_b = c(1)))
  right <- rel_from_df(con, data.frame(right_a = c(1, 2, 3), right_b = c(1, 1, 2)))
  cond <- list(expr_function("eq", list(expr_reference("left_b"), expr_reference("right_b"))))
  rel2 <- rel_join(left, right, cond, "right")
  rel_df <- rel_to_altrep(rel2)
  dim(rel_df)
  expected_result <- data.frame(left_b = c(1, 1, NA), right_a = c(1, 2, 3), right_b = c(1, 1, 2))
  expect_equal(rel_df, expected_result)
})

test_that("Full join returns all outer relations", {
  dbExecute(con, "CREATE OR REPLACE MACRO eq(a, b) AS a = b")
  left <- rel_from_df(con, data.frame(left_a = c(1, 2, 5), left_b = c(4, 5, 6)))
  right <- rel_from_df(con, data.frame(right_a = c(1, 2, 3), right_b = c(1, 1, 2)))
  cond <- list(expr_function("eq", list(expr_reference("left_a"), expr_reference("right_a"))))
  rel2 <- rel_join(left, right, cond, "outer")
  rel_df <- rel_to_altrep(rel2)
  dim(rel_df)
  expected_result <- data.frame(
    left_a = c(1, 2, 5, NA),
    left_b = c(4, 5, 6, NA),
    right_a = c(1, 2, NA, 3),
    right_b = c(1, 1, NA, 2)
  )
  expect_equal(rel_df, expected_result)
})

test_that("cross join works", {
  left <- rel_from_df(con, data.frame(left_a = c(1, 2, 3), left_b = c(1, 1, 2)))
  right <- rel_from_df(con, data.frame(right_a = c(1, 4, 5), right_b = c(7, 8, 9)))
  cross <- rel_join(left, right, list(), "cross")
  order_by <- rel_order(cross, list(expr_reference("right_a"), expr_reference("right_a")))
  rel_df <- rel_to_altrep(order_by)
  dim(rel_df)
  expected_result <- data.frame(
    left_a = c(1, 2, 3, 1, 2, 3, 1, 2, 3),
    left_b = c(1, 1, 2, 1, 1, 2, 1, 1, 2),
    right_a = c(1, 1, 1, 4, 4, 4, 5, 5, 5),
    right_b = c(7, 7, 7, 8, 8, 8, 9, 9, 9)
  )
  expect_equal(rel_df, expected_result)
})

test_that("semi join works", {
  left <- rel_from_df(con, data.frame(left_b = c(1, 5, 6)))
  right <- rel_from_df(con, data.frame(right_a = c(1, 2, 3), right_b = c(1, 1, 2)))
  cond <- list(expr_function("eq", list(expr_reference("left_b"), expr_reference("right_a"))))
  # select * from left semi join right on (left_b = right_a)
  rel2 <- rel_join(left, right, cond, "semi")
  rel_df <- rel_to_altrep(rel2)
  dim(rel_df)
  expected_result <- data.frame(left_b = c(1))
  expect_equal(rel_df, expected_result)
})

test_that("anti join works", {
  left <- rel_from_df(con, data.frame(left_b = c(1, 5, 6)))
  right <- rel_from_df(con, data.frame(right_a = c(1, 2, 3), right_b = c(1, 1, 2)))
  cond <- list(expr_function("eq", list(expr_reference("left_b"), expr_reference("right_a"))))
  # select * from left anti join right on (left_b = right_a)
  rel2 <- rel_join(left, right, cond, "anti")
  rel_df <- rel_to_altrep(rel2)
  dim(rel_df)
  expected_result <- data.frame(left_b = c(5, 6))
  expect_equal(rel_df, expected_result)
})

test_that("Union all does not immediately materialize", {
  test_df_a <- rel_from_df(con, data.frame(a = c("1", "2"), b = c("3", "4")))
  test_df_b <- rel_from_df(con, data.frame(a = c("5", "6"), b = c("7", "8")))
  rel <- rel_union_all(test_df_a, test_df_b)
  rel_df <- rel_to_altrep(rel)
  expect_false(df_is_materialized(rel_df))
  dim(rel_df)
  expect_true(df_is_materialized(rel_df))
})

test_that("Union all has the correct values", {
  test_df_a <- rel_from_df(con, data.frame(a = c("1", "2"), b = c("3", "4")))
  test_df_b <- rel_from_df(con, data.frame(a = c("5", "6"), b = c("7", "8")))
  rel <- rel_union_all(test_df_a, test_df_b)
  rel_df <- rel_to_altrep(rel)
  expect_false(df_is_materialized(rel_df))
  dim(rel_df)
  expect_true(df_is_materialized(rel_df))
  expected_result <- data.frame(a = c("1", "2", "5", "6"), b = c("3", "4", "7", "8"))
  expect_equal(rel_df, expected_result)
})

test_that("Union all keeps duplicates", {
  test_df_a2 <- rel_from_df(con, data.frame(a = c("1", "2"), b = c("3", "4")))
  test_df_b2 <- rel_from_df(con, data.frame(a = c("1", "2"), b = c("3", "4")))
  rel <- rel_union_all(test_df_a2, test_df_b2)
  rel_df <- rel_to_altrep(rel)
  dim(rel_df)
  expect_true(df_is_materialized(rel_df))
  expected_result <- data.frame(a = c("1", "2", "1", "2"), b = c("3", "4", "3", "4"))
  expect_equal(rel_df, expected_result)
})

test_that("Inner join returns all inner relations", {
  dbExecute(con, "CREATE OR REPLACE MACRO eq(a, b) AS a = b")
  left <- rel_from_df(con, data.frame(left_a = c(1, 2, 3), left_b = c(1, 1, 2)))
  right <- rel_from_df(con, data.frame(right_b = c(1, 3), right_c = c(4, 5)))
  cond <- list(expr_function("eq", list(expr_reference("left_b"), expr_reference("right_b"))))
  rel2 <- rel_join(left, right, cond, "inner")
  rel_df <- rel_to_altrep(rel2)
  dim(rel_df)
  expected_result <- data.frame(left_a = c(1, 2), left_b = c(1, 1), right_b = c(1, 1), right_c = c(4, 4))
  expect_equal(rel_df, expected_result)
})

test_that("ASOF join works", {
  dbExecute(con, "CREATE OR REPLACE MACRO gte(a, b) AS a >= b")
  test_df1 <- rel_from_df(con, data.frame(ts = c(1, 2, 3, 4, 5, 6, 7, 8, 9)))
  test_df2 <- rel_from_df(con, data.frame(event_ts = c(1, 3, 6, 8), event_id = c(0, 1, 2, 3)))
  cond <- list(expr_function("gte", list(expr_reference("ts"), expr_reference("event_ts"))))
  rel <- rel_join(test_df1, test_df2, cond, join_ref_type = "asof")
  rel_proj <- rel_project(rel, list(expr_reference("ts"), expr_reference("event_id")))
  rel_df <- rel_to_altrep(rel_proj)
  expected_result <- data.frame(ts = c(1, 2, 3, 4, 5, 6, 7, 8, 9), event_id = c(0, 0, 1, 1, 1, 2, 2, 3, 3))
  expect_equal(expected_result, rel_df)
})

test_that("LEFT ASOF join works", {
  dbExecute(con, "CREATE OR REPLACE MACRO gte(a, b) AS a >= b")
  test_df1 <- rel_from_df(con, data.frame(ts = c(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
  test_df2 <- rel_from_df(con, data.frame(event_ts = c(2, 4, 6, 8), event_id = c(0, 1, 2, 3)))
  cond <- list(expr_function("gte", list(expr_reference("ts"), expr_reference("event_ts"))))
  rel <- rel_join(test_df1, test_df2, cond, join = "left", join_ref_type = "asof")
  rel_proj <- rel_project(rel, list(expr_reference("ts"), expr_reference("event_ts"), expr_reference("event_id")))
  order <- rel_order(rel_proj, list(expr_reference("ts")))
  rel_df <- rel_to_altrep(order)
  expected_result <- data.frame(ts = c(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), event_ts = c(NA, NA, 2, 2, 4, 4, 6, 6, 8, 8), event_id = c(NA, NA, 0, 0, 1, 1, 2, 2, 3, 3))
  expect_equal(expected_result, rel_df)
})

test_that("Positional cross join works", {
  test_df1 <- rel_from_df(con, data.frame(a = c(11, 12, 13), b = c(1, 2, 3)))
  test_df2 <- rel_from_df(con, data.frame(c = c(11, 12), d = c(1, 2)))
  rel <- rel_join(test_df1, test_df2, list(), join = "cross", join_ref_type = "positional")
  rel_df <- rel_to_altrep(rel)
  expected_result <- data.frame(a = c(11, 12, 13), b = c(1, 2, 3), c = c(11, 12, NA), d = c(1, 2, NA))
  expect_equal(expected_result, rel_df)
})

test_that("regular positional join works", {
  dbExecute(con, "CREATE OR REPLACE MACRO eq(a, b) AS a = b")
  test_df1 <- rel_from_df(con, data.frame(a = c(11, 12, 13), b = c(1, 2, 3)))
  test_df2 <- rel_from_df(con, data.frame(c = c(11, 12, 14, 11), d = c(4, 5, 6, 8)))
  cond <- expr_function("eq", list(expr_reference("a"), expr_reference("c")))
  rel <- rel_join(test_df1, test_df2, list(cond), join_ref_type = "positional")
  rel_df <- rel_to_altrep(rel)
  expected_result <- data.frame(a = c(11, 12), b = c(1, 2), c = c(11, 12), d = c(4, 5))
  expect_equal(expected_result, rel_df)
})

test_that("Invalid asof join condition throws error", {
  dbExecute(con, "CREATE OR REPLACE MACRO neq(a, b) AS a <> b")
  test_df1 <- rel_from_df(con, data.frame(ts = c(1, 2, 3, 4, 5, 6, 7, 8, 9)))
  test_df2 <- rel_from_df(con, data.frame(begin = c(1, 3, 6, 8), value = c(0, 1, 2, 3)))
  cond <- list(expr_function("neq", list(expr_reference("ts"), expr_reference("begin"))))
  expect_error(rel_join(test_df1, test_df2, cond, join_ref_type = "asof"), "Binder Error")
})

test_that("multiple inequality conditions for asof join throws error", {
  dbExecute(con, "CREATE OR REPLACE MACRO gte(a, b) AS a >= b")
  test_df1 <- rel_from_df(con, data.frame(ts = c(1, 2, 3, 4, 5, 6, 7, 8, 9)))
  test_df2 <- rel_from_df(con, data.frame(begin = c(1, 3, 6, 8), value = c(0, 1, 2, 3)))
  cond1 <- expr_function("gte", list(expr_reference("ts"), expr_reference("begin")))
  cond2 <- expr_function("gte", list(expr_reference("ts"), expr_reference("value")))
  conds <- list(cond1, cond2)
  expect_error(rel_join(test_df1, test_df2, conds, join_ref_type = "asof"), "Binder Error")
})


test_that("Inequality joins work", {
  dbExecute(con, "CREATE OR REPLACE MACRO gte(a, b) AS a >= b")
  timing_df <- rel_from_df(con, data.frame(ts = c(1, 2, 3, 4, 5, 6)))
  events_df <- rel_from_df(con, data.frame(event_ts = c(1, 3, 6, 8), event_id = c(0, 1, 2, 3)))
  cond <- list(expr_function("gte", list(expr_reference("ts"), expr_reference("event_ts"))))
  rel <- rel_inner_join(timing_df, events_df, cond)
  rel_proj <- rel_project(rel, list(expr_reference("ts"), expr_reference("event_ts")))
  rel_order <- rel_order(rel_proj, list(expr_reference("ts"), expr_reference("event_ts")))
  rel_df <- rel_to_altrep(rel_order)
  expected_result <- data.frame(ts = c(1, 2, 3, 3, 4, 4, 5, 5, 6, 6, 6), event_ts = c(1, 1, 1, 3, 1, 3, 1, 3, 1, 3, 6))
  expect_equal(expected_result, rel_df)
})


test_that("Inequality join works to perform between operation", {
  dbExecute(con, "CREATE OR REPLACE MACRO gt(a, b) AS a > b")
  dbExecute(con, "CREATE OR REPLACE MACRO lt(a, b) AS a < b")
  timing_df <- rel_from_df(con, data.frame(ts = c(1, 2, 3, 4, 5, 6, 7, 8, 9)))
  events_df <- rel_from_df(con, data.frame(event_ts = c(1, 3, 6, 8), event_id = c(0, 1, 2, 3)))
  lead <- expr_function("lead", list(expr_reference("event_ts")))
  window_lead <- expr_window(lead, offset_expr = expr_constant(1))
  expr_set_alias(window_lead, "lead")
  proj_window <- rel_project(events_df, list(expr_reference("event_ts"), window_lead, expr_reference("event_id")))
  cond1 <- expr_function("gt", list(expr_reference("ts"), expr_reference("event_ts")))
  cond2 <- expr_function("lt", list(expr_reference("ts"), expr_reference("lead")))
  conds <- list(cond1, cond2)
  rel <- rel_inner_join(timing_df, proj_window, conds)
  rel_proj <- rel_project(rel, list(expr_reference("ts")))
  rel_order <- rel_order(rel_proj, list(expr_reference("ts")))
  rel_df <- rel_to_altrep(rel_order)
  expected_result <- data.frame(ts = c(2, 4, 5, 7))
  expect_equal(expected_result, rel_df)
})


# nobody should do this in reality. It's a pretty dumb idea
test_that("we can union the same relation to itself", {
  test_df_a2 <- rel_from_df(con, data.frame(a = c("1", "2"), b = c("3", "4")))
  rel <- rel_union_all(test_df_a2, test_df_a2)
  rel_df <- rel_to_altrep(rel)
  expected_result <- data.frame(a = c("1", "2", "1", "2"), b = c("3", "4", "3", "4"))
  expect_equal(rel_df, expected_result)
})

test_that("we throw an error when attempting to union all relations that are not compatible", {
  test_df_a2 <- rel_from_df(con, data.frame(a = c("1", "2"), b = c("3", "4")))
  test_df_b2 <- rel_from_df(con, data.frame(a = c("1", "2"), b = c("3", "4"), c = c("5", "6")))
  # The two data frames have different dimensions, therefore you get a binding error.
  expect_error(rel_union_all(test_df_a2, test_df_b2), "Binder Error")
})

test_that("A union with different column types casts to the richer type", {
  test_df_a1 <- rel_from_df(con, data.frame(a = c(1)))
  test_df_a2 <- rel_from_df(con, data.frame(a = c("1")))
  rel <- rel_union_all(test_df_a1, test_df_a2)
  res <- rapi_rel_to_df(rel)
  expected <- data.frame(a = c("1.0", "1"))
  expect_equal(class(res$a), class(expected$a))
  expect_equal(res$a[1], expected$a[1])
  expect_equal(res$a[2], expected$a[2])
})

test_that("Set Intersect returns set intersection", {
  test_df_a <- rel_from_df(con, data.frame(a = c(1, 2), b = c(3, 4)))
  test_df_b <- rel_from_df(con, data.frame(a = c(1, 6), b = c(3, 8)))
  rel <- rel_set_intersect(test_df_a, test_df_b)
  rel_df <- rel_to_altrep(rel)
  expect_false(df_is_materialized(rel_df))
  dim(rel_df)
  expected_result <- data.frame(a = c(1), b = c(3))
  expect_equal(rel_df, expected_result)
})

test_that("Set intersect casts columns to the richer type", {
  # df1 is Integer
  df1 <- data.frame(x = 1:4)
  # df2 has Double
  df2 <- data.frame(x = 4)
  expect_equal(class(df1$x), "integer")
  expect_equal(class(df2$x), "numeric")
  out <- rel_set_intersect(
    rel_from_df(con, df1),
    rel_from_df(con, df2)
  )
  out_df <- rapi_rel_to_df(out)
  expect_equal(class(out_df$x), "numeric")
})

test_that("Set Diff returns the set difference", {
  test_df_a <- rel_from_df(con, data.frame(a = c(1, 2), b = c(3, 4)))
  test_df_b <- rel_from_df(con, data.frame(a = c(1, 6), b = c(3, 8)))
  rel <- rel_set_diff(test_df_a, test_df_b)
  rel_df <- rel_to_altrep(rel)
  expect_false(df_is_materialized(rel_df))
  dim(rel_df)
  expected_result <- data.frame(a = c(2), b = c(4))
  expect_equal(rel_df, expected_result)
})

test_that("Symmetric difference returns the symmetric difference", {
  test_df_a <- rel_from_df(con, data.frame(a = c(1, 2), b = c(3, 4)))
  test_df_b <- rel_from_df(con, data.frame(a = c(1, 6), b = c(3, 8)))
  rel <- rel_set_symdiff(test_df_a, test_df_b)
  rel_df <- rel_to_altrep(rel)
  expect_false(df_is_materialized(rel_df))
  dim(rel_df)
  expected_result <- data.frame(a = c(2, 6), b = c(4, 8))
  expect_equal(rel_df, expected_result)
})

test_that("rel aggregate with no groups but a sum over a column, sums the column", {
  rel_a <- rel_from_df(con, data.frame(a = c(1, 2), b = c(3, 4)))
  aggrs <- list(sum = expr_function("sum", list(expr_reference("a"))))
  res <- rel_aggregate(rel_a, list(), aggrs)
  rel_df <- rel_to_altrep(res)
  expected_result <- data.frame(sum = c(3))
  expect_equal(rel_df, expected_result)
})

test_that("rel aggregate with groups and aggregate function works", {
  rel_a <- rel_from_df(con, data.frame(a = c(1, 2, 5, 5), b = c(3, 3, 4, 4)))
  aggrs <- list(sum = expr_function("sum", list(expr_reference("a"))))
  res <- rel_aggregate(rel_a, list(expr_reference("b")), aggrs)
  rel_df <- rel_to_altrep(res)
  expected_result <- data.frame(b = c(3, 4), sum = c(3, 10))
  expect_equal(rel_df, expected_result)
})

test_that("Window sum expression function test works", {
  #   select j, i, sum(i) over (partition by j) from a order by 1,2
  rel_a <- rel_from_df(con, data.frame(a = c(1:8), b = c(1, 1, 2, 2, 3, 3, 4, 4)))
  sum_func <- expr_function("sum", list(expr_reference("a")))
  aggrs <- expr_window(sum_func, partitions = list(expr_reference("b")))
  expr_set_alias(aggrs, "window_result")
  window_proj <- rel_project(rel_a, list(expr_reference("a"), aggrs))
  order_over_window <- rapi_rel_order(window_proj, list(expr_reference("window_result")))
  res <- rel_to_altrep(order_over_window)
  expected_result <- data.frame(a = c(1:8), window_result = c(3, 3, 7, 7, 11, 11, 15, 15))
  expect_equal(res, expected_result)
})

test_that("Window count function works", {
  #   select a, b, count(b) over (partition by a) from a order by a
  rel_a <- rel_from_df(con, data.frame(a = c(1:8), b = c(1, 1, 2, 2, 3, 3, 4, 4)))
  count_func <- expr_function("count", list(expr_reference("a")))
  count <- expr_window(count_func, partitions = list(expr_reference("b")))
  expr_set_alias(count, "window_result")
  window_proj <- rel_project(rel_a, list(count))
  res <- rel_to_altrep(window_proj)
  expected_result <- data.frame(window_result = c(2, 2, 2, 2, 2, 2, 2, 2))
  expect_equal(res, expected_result)
})

test_that("Window avg function works", {
  #     select a, b, avg(b) over (partition by a) from a order by a
  rel_a <- rel_from_df(con, data.frame(a = c(1:8), b = c(1, 1, 2, 2, 3, 3, 4, 4)))
  avg_func <- expr_function("avg", list(expr_reference("a")))
  avg_window <- expr_window(avg_func, partitions = list(expr_reference("b")))
  expr_set_alias(avg_window, "window_result")
  window_proj <- rel_project(rel_a, list(avg_window))
  ordered <- rel_order(window_proj, list(expr_reference("window_result")))
  res <- rel_to_altrep(ordered)
  expected_result <- data.frame(window_result = c(1.5, 1.5, 3.5, 3.5, 5.5, 5.5, 7.5, 7.5))
  expect_equal(res, expected_result)
})

test_that("Window sum with Partition, order, and window boundaries works", {
  #     SUM(x) OVER (partition by b ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
  rel_a <- rel_from_df(con, data.frame(a = c(1:8), b = c(1, 1, 1, 1, 2, 2, 2, 2)))
  partitions <- list(expr_reference("b"))
  order_by_a <- list(rapi_rel_order(rel_a, list(expr_reference("a"))))
  sum_func <- expr_function("sum", list(expr_reference("a")))
  sum_window <- expr_window(sum_func,
    partitions = partitions,
    order_bys = list(expr_reference("a")),
    window_boundary_start = "expr_preceding_rows",
    window_boundary_end = "current_row_rows",
    start_expr = expr_constant(2)
  )
  expr_set_alias(sum_window, "window_result")
  window_proj <- rel_project(rel_a, list(expr_reference("a"), sum_window))
  proj_order <- rel_order(window_proj, list(expr_reference("a")))
  res <- rel_to_altrep(proj_order)
  expected_result <- data.frame(a = c(1:8), window_result = c(1, 3, 6, 9, 5, 11, 18, 21))
  expect_equal(res, expected_result)
})

test_that("Window boundaries boundaries are CaSe INsenSItive", {
  #     SUM(x) OVER (partition by b ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
  rel_a <- rel_from_df(con, data.frame(a = c(1:8), b = c(1, 1, 1, 1, 2, 2, 2, 2)))
  partitions <- list(expr_reference("b"))
  order_by_a <- list(rapi_rel_order(rel_a, list(expr_reference("a"))))
  sum_func <- expr_function("sum", list(expr_reference("a")))
  sum_window <- expr_window(sum_func,
    partitions = partitions,
    order_bys = list(expr_reference("a")),
    window_boundary_start = "exPr_PREceding_rOWs",
    window_boundary_end = "cURrEnt_rOw_RoWs",
    start_expr = expr_constant(2)
  )
  expr_set_alias(sum_window, "window_result")
  window_proj <- rel_project(rel_a, list(expr_reference("a"), sum_window))
  proj_order <- rel_order(window_proj, list(expr_reference("a")))
  res <- rel_to_altrep(proj_order)
  expected_result <- data.frame(a = c(1:8), window_result = c(1, 3, 6, 9, 5, 11, 18, 21))
  expect_equal(res, expected_result)
})

test_that("Window avg with a filter expression and partition works", {
  #   select a, b, avg(a) FILTER (WHERE x % 2 = 0) over (partition by b)
  DBI::dbExecute(con, "CREATE OR REPLACE MACRO mod(a, b) as a % b")
  DBI::dbExecute(con, "CREATE OR REPLACE MACRO eq(a, b) as a = b")
  rel_a <- rel_from_df(con, data.frame(a = c(1:8), b = c(1, 1, 2, 2, 3, 3, 4, 4)))
  partitions <- list(expr_reference("b"))
  mod_function <- expr_function("mod", list(expr_reference("a"), expr_constant(2)))
  zero <- expr_constant(0)
  filters <- list(expr_function("eq", list(zero, mod_function)))
  avg_func <- expr_function("avg", args = list(expr_reference("a")), filter_bys = filters)
  avg_filter_window <- expr_window(avg_func, partitions = partitions)
  expr_set_alias(avg_filter_window, "avg_filter")
  window_proj <- rel_project(rel_a, list(avg_filter_window))
  proj_order <- rel_order(window_proj, list(expr_reference("avg_filter")))
  expected_result <- data.frame(avg_filter = c(2, 2, 4, 4, 6, 6, 8, 8))
  res <- rel_to_altrep(proj_order)
  expect_equal(res, expected_result)
})

test_that("Window lag function works as expected", {
  #   select a, b, lag(a, 1) OVER () order by a
  rel_a <- rel_from_df(con, data.frame(a = c(1:8), b = c(1, 1, 2, 2, 3, 3, 4, 4)))
  lag <- expr_function("lag", list(expr_reference("a")))
  window_lag <- expr_window(lag, offset_expr = expr_constant(1))
  expr_set_alias(window_lag, "lag")
  proj_window <- rel_project(rel_a, list(expr_reference("a"), window_lag))
  order_over_window <- rapi_rel_order(proj_window, list(expr_reference("a")))
  expected_result <- data.frame(a = c(1:8), lag = c(NA, 1, 2, 3, 4, 5, 6, 7))
  res <- rel_to_altrep(order_over_window)
  expect_equal(res, expected_result)
})


test_that("function name for window is case insensitive", {
  #   select a, b, lag(a, 1) OVER () order by a
  rel_a <- rel_from_df(con, data.frame(a = c(1:8), b = c(1, 1, 2, 2, 3, 3, 4, 4)))
  lag <- expr_function("LAG", list(expr_reference("a")))
  window_lag <- expr_window(lag, offset_expr = expr_constant(1))
  expr_set_alias(window_lag, "lag")
  proj_window <- rel_project(rel_a, list(expr_reference("a"), window_lag))
  order_over_window <- rapi_rel_order(proj_window, list(expr_reference("a")))
  expected_result <- data.frame(a = c(1:8), lag = c(NA, 1, 2, 3, 4, 5, 6, 7))
  res <- rel_to_altrep(order_over_window)
  expect_equal(res, expected_result)
})

test_that("Window lead function works as expected", {
  #   select a, b, lag(a, 1) OVER () order by a
  rel_a <- rel_from_df(con, data.frame(a = c(1:8), b = c(1, 1, 2, 2, 3, 3, 4, 4)))
  lead <- expr_function("lead", list(expr_reference("a")))
  window_lead <- expr_window(lead, offset_expr = expr_constant(1))
  expr_set_alias(window_lead, "lead")
  proj_window <- rel_project(rel_a, list(expr_reference("a"), window_lead))
  order_over_window <- rapi_rel_order(proj_window, list(expr_reference("a")))
  expected_result <- data.frame(a = c(1:8), lead = c(2, 3, 4, 5, 6, 7, 8, NA))
  res <- rel_to_altrep(order_over_window)
  expect_equal(res, expected_result)
})

test_that("Window function with string aggregate works", {
  #   select j, s, string_agg(s, '|') over (partition by b) from a order by j, s;
  rel_a <- rel_from_df(con, data.frame(r = c(1, 2, 3, 4), a = c("hello", "Big", "world", "42"), b = c(1, 1, 2, 2)))
  str_agg <- expr_function("string_agg", list(expr_reference("a")))
  partitions <- list(expr_reference("b"))
  window_str_cat <- expr_window(str_agg, partitions = partitions)
  expr_set_alias(window_str_cat, "str_agg_res")
  proj_window <- rel_project(rel_a, list(expr_reference("r"), window_str_cat))
  order_over_window <- rapi_rel_order(proj_window, list(expr_reference("r")))
  expected_result <- data.frame(r = c(1:4), str_agg_res = c("hello,Big", "hello,Big", "world,42", "world,42"))
  res <- rel_to_altrep(order_over_window)
  expect_equal(res, expected_result)
})

test_that("You can perform window functions on row_number", {
  # select a, b, row_number() OVER () from tmp order by a;
  rel_a <- rel_from_df(con, data.frame(a = c(8:1), b = c(1, 1, 2, 2, 3, 3, 4, 4)))
  row_number <- expr_function("row_number", list())
  window_function <- expr_window(row_number)
  expr_set_alias(window_function, "row_number")
  proj <- rel_project(rel_a, list(expr_reference("a"), window_function))
  order_by_a <- rel_order(proj, list(expr_reference("a")))
  expected_result <- data.frame(a = c(1:8), row_number = (8:1))
  res <- rel_to_altrep(order_by_a)
  expect_equal(res, expected_result)
})

# also tests order by inside the rank function.
# these tests come from https://dplyr.tidyverse.org/articles/window-functions.html
# in dplyr min_rank = rank
test_that("You can perform the window function min_rank", {
  # select rank() OVER (order by a) from t1
  rel_a <- rel_from_df(con, data.frame(a = c(1, 1, 2, 2, 2)))
  rank_func <- expr_function("rank", list())
  min_rank_window <- expr_window(rank_func, order_bys = list(expr_reference("a")))
  expr_set_alias(min_rank_window, "window_result")
  window_proj <- rel_project(rel_a, list(expr_reference("a"), min_rank_window))
  res <- rel_to_altrep(window_proj)
  expected_result <- data.frame(a = c(1, 1, 2, 2, 2), window_result = c(1, 1, 3, 3, 3))
  expect_equal(res, expected_result)
})

test_that("You can perform the window function dense_rank", {
  # select dense_rank() OVER (order by a) from t1;
  rel_a <- rel_from_df(con, data.frame(a = c(1, 1, 2, 2, 2)))
  dense_rank_fun <- expr_function("dense_rank", list())
  min_rank_window <- expr_window(dense_rank_fun, order_bys = list(expr_reference("a")))
  expr_set_alias(min_rank_window, "window_result")
  window_proj <- rel_project(rel_a, list(expr_reference("a"), min_rank_window))
  res <- rel_to_altrep(window_proj)
  expected_result <- data.frame(a = c(1, 1, 2, 2, 2), window_result = c(1, 1, 2, 2, 2))
  expect_equal(res, expected_result)
})

test_that("You can perform the window function cume_dist", {
  # select cume_dist() OVER (order by a) from t1;
  rel_a <- rel_from_df(con, data.frame(a = c(1, 1, 2, 2, 2)))
  cume_dist_func <- expr_function("cume_dist", list())
  cume_dist_window <- expr_window(cume_dist_func, order_bys = list(expr_reference("a")))
  expr_set_alias(cume_dist_window, "cume_dist")
  window_proj <- rel_project(rel_a, list(expr_reference("a"), cume_dist_window))
  order_proj <- rel_order(window_proj, list(expr_reference("a")))
  res <- rel_to_altrep(order_proj)
  expected_result <- data.frame(a = c(1, 1, 2, 2, 2), cume_dist = c(0.4, 0.4, 1.0, 1.0, 1.0))
  expect_equal(res, expected_result)
})

test_that("You can perform the window function percent rank", {
  # select percent_rank() OVER (order by a) from t1;
  rel_a <- rel_from_df(con, data.frame(a = c(5, 1, 3, 2, 2)))
  percent_rank_func <- expr_function("percent_rank", list())
  percent_rank_wind <- expr_window(percent_rank_func, order_bys = list(expr_reference("a")))
  expr_set_alias(percent_rank_wind, "percent_rank")
  window_proj <- rel_project(rel_a, list(expr_reference("a"), percent_rank_wind))
  order_proj <- rel_order(window_proj, list(expr_reference("a")))
  res <- rel_to_altrep(order_proj)
  expected_result <- data.frame(a = c(1, 2, 2, 3, 5), percent_rank = c(0.00, 0.25, 0.25, 0.75, 1.00))
  expect_equal(res, expected_result)
})

# with and without offsets
test_that("R semantics for adding NaNs is respected", {
  dbExecute(con, "CREATE OR REPLACE MACRO eq(a, b) AS a = b")
  test_df_a <- rel_from_df(con, data.frame(a = c(1, 2), b = c(3, 4)))
  test_df_b <- rel_from_df(con, data.frame(c = c(NaN, 6), d = c(3, 8)))
  cond <- list(expr_function("eq", list(expr_reference("b"), expr_reference("d"))))
  rel_join <- rel_join(test_df_a, test_df_b, cond, "inner")
  addition_expression <- expr_function("+", list(expr_reference("a"), expr_reference("c")))
  proj <- rel_project(rel_join, list(addition_expression))
  res <- rapi_rel_to_df(proj)
  expect_true(is.na(res[[1]]))
})

test_that("R semantics for arithmetics sum function are respected", {
  test_df_a <- rel_from_df(con, data.frame(a = c(1:5, NA)))
  sum_rel <- expr_function("sum", list(expr_reference("a")))
  ans <- rel_aggregate(test_df_a, list(), list(sum_rel))
  res <- rel_to_altrep(ans)
  expect_equal(res[[1]], 15)
})

test_that("anti joins for eq_na_matches works", {
  dbExecute(con, 'CREATE OR REPLACE MACRO "___eq_na_matches_na"(a, b) AS ((a IS NULL AND b IS NULL) OR (a = b))')
  rel1 <- rel_from_df(con, data.frame(x = c(1, 1, 2, 3)))
  rel2 <- rel_from_df(con, data.frame(y = c(2, 3, 3, 4)))
  cond <- list(expr_function("___eq_na_matches_na", list(expr_reference("x"), expr_reference("y"))))
  out <- rel_join(rel1, rel2, cond, "anti")
  res <- rel_to_altrep(out)
  expect_equal(res, data.frame(x = c(1, 1)))
})

test_that("semi joins for eq_na_matches works", {
  dbExecute(con, 'CREATE OR REPLACE MACRO "___eq_na_matches_na"(a, b) AS ((a IS NULL AND b IS NULL) OR (a = b))')
  rel1 <- rel_from_df(con, data.frame(x = c(1, 1, 2, 2)))
  rel2 <- rel_from_df(con, data.frame(y = c(2, 2, 2, 2, 3, 3, 3)))
  cond <- list(expr_function("___eq_na_matches_na", list(expr_reference("x"), expr_reference("y"))))
  out <- rel_join(rel1, rel2, cond, "semi")
  res <- rel_to_altrep(out)
  expect_equal(res, data.frame(x = c(2, 2)))
})

test_that("rel_project does not automatically quote upper-case column names", {
  df <- data.frame(B = 1)
  rel <- rel_from_df(con, df)
  ref <- expr_reference(names(df))
  exprs <- list(ref)
  proj <- rel_project(rel, exprs)
  ans <- rapi_rel_to_altrep(proj)
  expect_equal(df, ans)
})

test_that("rel_to_sql works for row_number", {
  invisible(DBI::dbExecute(con, "CREATE MACRO \"==\"(a, b) AS a = b"))
  df1 <- data.frame(a = 1)
  rel1 <- rel_from_df(con, df1)
  rel2 <- rel_project(
    rel1,
    list({
      tmp_expr <- expr_window(expr_function("row_number", list()), list(), list(), offset_expr = NULL, default_expr = NULL)
      expr_set_alias(tmp_expr, "___row_number")
      tmp_expr
    })
  )
  sql <- rel_to_sql(rel2)
  sub_str_sql <- substr(sql, 0, 44)
  expect_equal(sub_str_sql, "SELECT row_number() OVER () AS ___row_number")
})

test_that("rel_from_table_function works", {
  rel <- rel_from_table_function(default_connection(), "generate_series", list(1L, 10L, 2L))
  df <- as.data.frame(rel)
  expect_equal(df$generate_series, c(1, 3, 5, 7, 9))
})

test_that("we don't crash with evaluation errors", {
  invisible(DBI::dbExecute(con, "CREATE OR REPLACE MACRO \"==\"(x, y) AS x = y"))
  df1 <- data.frame(a = "a")

  rel1 <- rel_from_df(con, df1)
  rel2 <- rel_project(
    rel1,
    list(
      {
        tmp_expr <- expr_reference("a")
        expr_set_alias(tmp_expr, "a")
        tmp_expr
      },
      {
        tmp_expr <- expr_function(
          "==",
          list(expr_reference("a"), expr_constant(1))
        )
        expr_set_alias(tmp_expr, "b")
        tmp_expr
      }
    )
  )

  ans <- rapi_rel_to_altrep(rel2)

  # This query is supposed to throw a runtime error.
  # If this succeeds, find a new query that throws a runtime error.
  expect_error(nrow(ans), "Error evaluating duckdb query")
})
