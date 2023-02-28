library("DBI")
library("testthat")

con <- dbConnect(duckdb())
on.exit(dbDisconnect(con, shutdown = TRUE))

test_that("we can create a relation from a df", {
  rel_from_df(con, mtcars)
  expect_true(TRUE)
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
  expect_true(any(grepl("DUCKDB_ALTREP_REL_VECTOR", inspect_output, fixed=TRUE)))
  expect_true(any(grepl("DUCKDB_ALTREP_REL_ROWNAMES", inspect_output, fixed=TRUE)))
  expect_false(df_is_materialized(df))
  dim(df)
  expect_true(df_is_materialized(df))
  expect_equal(iris, df)
})

test_that("the altrep-conversion for relations work for weirdo types", {
  test_df <- data.frame(col_date=as.Date("2019-11-26"), col_ts=as.POSIXct("2019-11-26 21:11Z", "UTC"), col_factor=factor(c("a")))
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
    left <- rel_from_df(con, data.frame(left_a=c(1, 2, 3), left_b=c(1, 1, 2)))
    right <- rel_from_df(con, data.frame(right_b=c(1, 3), right_c=c(4, 5)))
    cond <- list(expr_function("eq", list(expr_reference("left_b"), expr_reference("right_b"))))
    rel2 <- rel_join(left, right, cond, "inner")
    rel_df <- rel_to_altrep(rel2)
    dim(rel_df)
    expected_result <- data.frame(left_a=c(1, 2), left_b=c(1, 1), right_b=c(1, 1), right_c=c(4, 4))
    expect_equal(rel_df, expected_result)
})


test_that("Left join returns all left relations", {
    dbExecute(con, "CREATE OR REPLACE MACRO eq(a, b) AS a = b")
    left <- rel_from_df(con, data.frame(left_a=c(1, 2, 3), left_b=c(1, 1, 2)))
    right <- rel_from_df(con, data.frame(right_b=c(1)))
    cond <- list(expr_function("eq", list(expr_reference("left_b"), expr_reference("right_b"))))
    rel2 <- rel_join(left, right, cond, "left")
    rel_df <- rel_to_altrep(rel2)
    dim(rel_df)
    expected_result <- data.frame(left_a=c(1, 2, 3), left_b=c(1, 1, 2), right_b=c(1, 1, NA))
    expect_equal(rel_df, expected_result)
})

test_that("Right join returns all right relations", {
    dbExecute(con, "CREATE OR REPLACE MACRO eq(a, b) AS a = b")
    left <- rel_from_df(con, data.frame(left_b=c(1)))
    right <- rel_from_df(con, data.frame(right_a=c(1, 2, 3), right_b=c(1, 1, 2)))
    cond <- list(expr_function("eq", list(expr_reference("left_b"), expr_reference("right_b"))))
    rel2 <- rel_join(left, right, cond, "right")
    rel_df <- rel_to_altrep(rel2)
    dim(rel_df)
    expected_result <- data.frame(left_b=c(1, 1, NA), right_a=c(1, 2, 3), right_b=c(1, 1, 2))
    expect_equal(rel_df, expected_result)
})

test_that("Full join returns all outer relations", {
    dbExecute(con, "CREATE OR REPLACE MACRO eq(a, b) AS a = b")
    left <- rel_from_df(con, data.frame(left_a=c(1, 2, 5), left_b=c(4, 5, 6)))
    right <- rel_from_df(con, data.frame(right_a=c(1, 2, 3), right_b=c(1, 1, 2)))
    cond <- list(expr_function("eq", list(expr_reference("left_a"), expr_reference("right_a"))))
    rel2 <- rel_join(left, right, cond, "outer")
    rel_df <- rel_to_altrep(rel2)
    dim(rel_df)
    expected_result <- data.frame(left_a=c(1, 2, 5, NA),
                                  left_b=c(4, 5, 6, NA),
                                  right_a=c(1, 2, NA, 3),
                                  right_b=c(1, 1, NA, 2))
    expect_equal(rel_df, expected_result)
})


test_that("Union all does not immediately materialize", {
    test_df_a <- rel_from_df(con, data.frame(a=c('1', '2'), b=c('3', '4')))
    test_df_b <- rel_from_df(con, data.frame(a=c('5', '6'), b=c('7', '8')))
    rel <- rel_union_all(test_df_a, test_df_b)
    rel_df <- rel_to_altrep(rel)
    expect_false(df_is_materialized(rel_df))
    dim(rel_df)
    expect_true(df_is_materialized(rel_df))
})

test_that("Union all has the correct values", {
    test_df_a <- rel_from_df(con, data.frame(a=c('1', '2'), b=c('3', '4')))
    test_df_b <- rel_from_df(con, data.frame(a=c('5', '6'), b=c('7', '8')))
    rel <- rel_union_all(test_df_a, test_df_b)
    rel_df <- rel_to_altrep(rel)
    expect_false(df_is_materialized(rel_df))
    dim(rel_df)
    expect_true(df_is_materialized(rel_df))
    expected_result <- data.frame(a=c('1', '2', '5', '6'), b=c('3', '4', '7', '8'))
    expect_equal(rel_df, expected_result)
})

test_that("Union all keeps duplicates", {
    test_df_a2 <- rel_from_df(con, data.frame(a=c('1', '2'), b=c('3', '4')))
    test_df_b2 <- rel_from_df(con, data.frame(a=c('1', '2'), b=c('3', '4')))
    rel <- rel_union_all(test_df_a2, test_df_b2)
    rel_df <- rel_to_altrep(rel)
    dim(rel_df)
    expect_true(df_is_materialized(rel_df))
    expected_result <- data.frame(a=c('1', '2', '1', '2'), b=c('3', '4', '3', '4'))
    expect_equal(rel_df, expected_result)
})

# nobody should do this in reality. It's a pretty dumb idea
test_that("we can union the same relation to itself", {
     test_df_a2 <- rel_from_df(con, data.frame(a=c('1', '2'), b=c('3', '4')))
     rel <- rel_union_all(test_df_a2, test_df_a2)
     rel_df <- rel_to_altrep(rel)
     expected_result <- data.frame(a=c('1', '2', '1', '2'), b=c('3', '4', '3', '4'))
     expect_equal(rel_df, expected_result)
})

test_that("we throw an error when attempting to union all relations that are not compatible", {
    test_df_a2 <- rel_from_df(con, data.frame(a=c('1', '2'), b=c('3', '4')))
    test_df_b2 <- rel_from_df(con, data.frame(a=c('1', '2'), b=c('3', '4'), c=c('5', '6')))
    # The two data frames have different dimensions, therefore you get a binding error.
    expect_error(rel_union_all(test_df_a2, test_df_b2), "Binder Error")
})

test_that("A union with different column types throws an error", {
     test_df_a1 <- rel_from_df(con, data.frame(a=c(1)))
     test_df_a2 <- rel_from_df(con, data.frame(a=c('1')))
     rel <- rel_union_all(test_df_a1, test_df_a2)
     expect_error(rapi_rel_to_df(rel), "Invalid Error: Result mismatch in query!")
})

test_that("Set Intersect returns set intersection", {
    test_df_a <- rel_from_df(con, data.frame(a=c(1, 2), b=c(3, 4)))
    test_df_b <- rel_from_df(con, data.frame(a=c(1, 6), b=c(3, 8)))
    rel <- rel_set_intersect(test_df_a, test_df_b)
    rel_df <- rel_to_altrep(rel)
    expect_false(df_is_materialized(rel_df))
    dim(rel_df)
    expected_result <- data.frame(a=c(1), b=c(3))
    expect_equal(rel_df, expected_result)
})

test_that("Set Diff returns the set difference", {
    test_df_a <- rel_from_df(con, data.frame(a=c(1, 2), b=c(3, 4)))
    test_df_b <- rel_from_df(con, data.frame(a=c(1, 6), b=c(3, 8)))
    rel <- rel_set_diff(test_df_a, test_df_b)
    rel_df <- rel_to_altrep(rel)
    expect_false(df_is_materialized(rel_df))
    dim(rel_df)
    expected_result <- data.frame(a=c(2), b=c(4))
    expect_equal(rel_df, expected_result)
})

test_that("Symmetric difference returns the symmetric difference", {
    test_df_a <- rel_from_df(con, data.frame(a=c(1, 2), b=c(3, 4)))
    test_df_b <- rel_from_df(con, data.frame(a=c(1, 6), b=c(3, 8)))
    rel <- rel_set_symdiff(test_df_a, test_df_b)
    rel_df <- rel_to_altrep(rel)
    expect_false(df_is_materialized(rel_df))
    dim(rel_df)
    expected_result <- data.frame(a=c(2, 6), b=c(4, 8))
    expect_equal(rel_df, expected_result)
})

test_that("rel aggregate with no groups but a sum over a column, sums the column", {
   rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1, 2), b=c(3, 4)))
   aggrs <- list(sum = duckdb:::expr_function("sum", list(duckdb:::expr_reference("a"))))
   res <- duckdb:::rel_aggregate(rel_a, list(), aggrs)
   rel_df <- duckdb:::rel_to_altrep(res)
   expected_result <- data.frame(sum=c(3))
   expect_equal(rel_df, expected_result)
})

test_that("rel aggregate with groups and aggregate function works", {
   rel_a <- rel_from_df(con, data.frame(a=c(1, 2, 5, 5), b=c(3, 3, 4, 4)))
   aggrs <- list(sum = expr_function("sum", list(expr_reference("a"))))
   res <- rel_aggregate(rel_a, list(expr_reference("b")), aggrs)
   rel_df <- rel_to_altrep(res)
   expected_result <- data.frame(b=c(3, 4), sum=c(3, 10))
   expect_equal(rel_df, expected_result)
})

test_that("Window function works", {
    # select j, i, sum(i) over () from a order by 1,2
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:2, 2, 1:4)))
    aggrs <- list(sum = expr_function("sum", list(expr_reference("a"))))
    #                    DF          GROUP BY CLAUSE        aggregation function.
    res <- rel_aggregate(rel_a, list(expr_reference("b")), aggrs)
    window_rel <- duckdb:::rel_window("aggregation_function", "partitions", "bounds")
#     widow_rel <- duckdb:::rel_window(window_function, table, order by clause);
    rel_df <- duckdb:::rel_to_altrep(test_df_a)
    #tibble(a = c(1:2, 2, 1:4)) |> mutate(dupe_id = row_number(), count = n(), .by = a)
})

test_that("Window sum function works", {
#     select j, i, sum(i) over (partition by j) from a order by 1,2
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    sum <- list(duckdb:::expr_reference("a"))
    partitions <- list(duckdb:::expr_reference("b"))
    window_function <- duckdb:::rapi_rel_window_aggregation(rel_a, "sum", sum, "a_sum", partitions, list(), list(), list())
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    expected_result <- data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4), a_sum=c(3, 3, 7, 7, 11, 11, 15, 15))
    res = duckdb:::rel_to_altrep(order_over_window)
    expect_equal(res, expected_result)
})

test_that("Window avg function works", {
#     select j, i, sum(i) over (partition by j) from a order by 1,2
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    sum <- list(duckdb:::expr_reference("a"))
    partitions <- list(duckdb:::expr_reference("b"))
    window_function <- duckdb:::rapi_rel_window_aggregation(rel_a, "sum", sum, "a_sum", partitions, list(), list(), list())
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    expected_result <- data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4), a_sum=c(1.5, 1.5, 3.5, 3.5, 5.5, 5.5, 7.5, 7.5))
    res = duckdb:::rel_to_altrep(order_over_window)
    expect_equal(res, expected_result)
})

test_that("You can project from a window function", {
#     select j, i, sum(i) over (partition by j) from a order by 1,2
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    sum <- list(duckdb:::expr_reference("a"))
    partitions <- list(duckdb:::expr_reference("b"))
    window_function <- duckdb:::rapi_rel_window_aggregation(rel_a, "sum", sum, "a_sum", partitions, list(), list(), list())
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    proj <- duckdb:::rapi_rel_project(order_over_window, list(duckdb:::expr_reference("a_sum")))
    expected_result <- data.frame(a_sum=c(3, 3, 7, 7, 11, 11, 15, 15))
    res = duckdb:::rel_to_altrep(proj)
    expect_equal(res, expected_result)
})

test_that("You can perform window functions on row_number", {
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(8:1),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    sum <- list(duckdb:::expr_reference("a"))
    partitions <- list(duckdb:::expr_reference("b"))
    window_function <- duckdb:::rapi_rel_window_aggregation(rel_a, "row_number", list(), "row_number", list(), list(), list(), list())
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    res = duckdb:::rel_to_altrep(order_over_window)
    expected_result <- data.frame(a=c(1:8), b=(4, 4, 3, 3, 2, 2, 1, 1), row_number=(8:1))
    expect_equal(res, expected_result)
})

# in dplyr min_rank = rank
# here
test_that("You can perform the window function min_rank", {
	con <- dbConnect(duckdb::duckdb())
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1, 1, 2, 2, 2)))
    order_by_a <- duckdb:::rapi_rel_order(rel_a, list(duckdb:::expr_reference("a")))
    window_function <- duckdb:::rapi_rel_window_aggregation(rel_a, "rank", list(), "rank", list(), order_by_a, list(), list())
	window_res <- duckdb:::rel_to_altrep(window_function)
    res = duckdb:::rel_to_altrep(order_over_window)
    expect_equal(res, expected_result)
})

test_that("You can perform the window function dense_rank", {
   con <- dbConnect(duckdb::duckdb())
   rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1, 1, 2, 2, 2)))
   order_by_a <- duckdb:::rapi_rel_order(rel_a, list(duckdb:::expr_reference("a")))
   window_function <- duckdb:::rapi_rel_window_aggregation(rel_a, "dense_rank", list(), "dense_rank", list(), order_by_a, list(), list())
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    res = duckdb:::rel_to_altrep(order_over_window)
    expect_equal(res, expected_result)
})


test_that("You can perform the window function cume_dist", {
   con <- dbConnect(duckdb::duckdb())
       rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1, 1, 2, 2, 2)))
       order_by_a <- duckdb:::rapi_rel_order(rel_a, list(duckdb:::expr_reference("a")))
       window_function <- duckdb:::rapi_rel_window_aggregation(rel_a, "rank", list(), "rank", list(), order_by_a, list(), list())
    order_by_a <- duckdb:::rapi_rel_order(rel_a, duckdb:::expr_reference("a"))
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    res = duckdb:::rel_to_altrep(order_over_window)
    expect_equal(res, expected_result)
})

test_that("You can perform the window function percent rank", {
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(8:1),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    sum <- list(duckdb:::expr_reference("a"))
    partitions <- list(duckdb:::expr_reference("b"))
    window_function <- duckdb:::rapi_rel_window_aggregation(rel_a, "percent_rank", list(), "percent_rank", list(), list(), list(), list())
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    res = duckdb:::rel_to_altrep(order_over_window)
    expect_equal(res, expected_result)
})
