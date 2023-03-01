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

test_that("Window sum function works", {
#     select j, i, sum(i) over (partition by j) from a order by 1,2
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    sum <- list(duckdb:::expr_reference("a"))
    partitions <- list(duckdb:::expr_reference("b"))
    window_function <- duckdb:::rel_window(rel=rel_a, window_function="sum", window_alias="a_sum", partitions=partitions, children=sum)
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    expected_result <- data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4), a_sum=c(3, 3, 7, 7, 11, 11, 15, 15))
    res = duckdb:::rel_to_altrep(order_over_window)
    expect_equal(res, expected_result)
})

test_that("Window count function works", {
#     select a, b, count(b) over (partition by a) from a order by a
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    count <- list(duckdb:::expr_reference("a"))
    partitions <- list(duckdb:::expr_reference("b"))
    window_function <- duckdb:::rel_window(rel=rel_a, window_function="count", window_alias="count", children=count, partitions=partitions)
    a_ref <- list(duckdb:::expr_reference("a", window_function))
    window_order_by_a <- duckdb:::rapi_rel_order(window_function, a_ref)
    expected_result <- data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4), count=c(2, 2, 2, 2, 2, 2, 2, 2))
    res = duckdb:::rel_to_altrep(window_order_by_a)
    expect_equal(res, expected_result)
})

test_that("Window avg function works", {
#     select a, b, avg(b) over (partition by a) from a order by a
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    avg <- list(duckdb:::expr_reference("a"))
    partitions <- list(duckdb:::expr_reference("b"))
    window_function <- duckdb:::rel_window(rel=rel_a, window_function="avg", window_alias="a_avg", children=avg, partitions=partitions)
    a_ref <- list(duckdb:::expr_reference("a", window_function))
    window_order_by_a <- duckdb:::rapi_rel_order(window_function, a_ref)
    expected_result <- data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4), a_avg=c(1.5, 1.5, 3.5, 3.5, 5.5, 5.5, 7.5, 7.5))
    res <- duckdb:::rel_to_altrep(window_order_by_a)
    expect_equal(res, expected_result)
})

test_that("Window avg with a filter expression and partition works", {
#   select a, b, avg(a) FILTER (WHERE x % 2 = 0) over (partition by b)
    DBI::dbExecute(con, "CREATE OR REPLACE MACRO mod(a, b) as a % b")
    DBI::dbExecute(con, "CREATE OR REPLACE MACRO eq(a, b) as a = b")
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    avg <- list(duckdb:::expr_reference("a"))
    partitions <- list(duckdb:::expr_reference("b"))
    mod_function <- duckdb:::expr_function("mod", list(duckdb:::expr_reference("a"), duckdb:::expr_constant(2)))
    zero <- duckdb:::expr_constant(0)
    filter_expr <- list(duckdb:::rel_filter(rel_a, list(duckdb:::expr_function("eq", list(zero, mod_function)))))
    window_function <- duckdb:::rel_window(rel=rel_a, window_function="avg", window_alias="a_avg", children=avg, partitions=partitions, filter_expr=filter_expr)
    a_ref <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, a_ref)
    expected_result <- data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4), a_avg=c(2, 2, 4, 4, 6, 6, 8, 8))
    res <- duckdb:::rel_to_altrep(order_over_window)
    expect_equal(res, expected_result)
})

test_that("Window sum with order by and bounds works", {
#    select a, b, SUM(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
#   add the current row and the previous row
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    sum <- list(duckdb:::expr_reference("a"))
    order_by_a <- list(duckdb:::rapi_rel_order(rel_a, list(duckdb:::expr_reference("a"))))
    window_function <- duckdb:::rel_window(rel=rel_a,
                                window_function="sum",
                                window_alias="window_res",
                                orders=order_by_a,
                                children=sum,
                                window_boundary_start="expr_preceding_rows",
                                window_boundary_end="current_row_rows",
                                start_expr=list(duckdb:::expr_constant(1)))
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    expected_result <- data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4), window_res=c(1, 3, 5, 7, 9, 11, 13, 15))
    res <- duckdb:::rel_to_altrep(order_over_window)
    expect_equal(res, expected_result)
})

test_that("Window sum with Parition, order, and window boundaries works", {
#     SUM(x) OVER (ORDER BY rowid ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:8),b=c(1, 1, 1, 1, 2, 2, 2, 2)))
    partitions <- list(duckdb:::expr_reference("b"))
    sum <- list(duckdb:::expr_reference("a"))
    order_by_a <- list(duckdb:::rapi_rel_order(rel_a, list(duckdb:::expr_reference("a"))))
    window_function <- duckdb:::rel_window(rel=rel_a,
                                window_function="sum",
                                window_alias="window_res",
                                orders=order_by_a,
                                partitions=partitions,
                                children=sum,
                                window_boundary_start="expr_preceding_rows",
                                window_boundary_end="current_row_rows",
                                start_expr=list(duckdb:::expr_constant(3)))
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    expected_result <- data.frame(a=c(1:8),b=c(1, 1, 1, 1, 2, 2, 2, 2), window_res=c(1, 3, 6, 10, 5, 11, 18, 26))
    res <- duckdb:::rel_to_altrep(order_over_window)
    expect_equal(res, expected_result)
})

test_that("Window lag function works as expected", {
#   select a, b, lag(a, 1) OVER () order by a
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    lag <- list(duckdb:::expr_reference("a"))
    window_function <- duckdb:::rel_window(rel=rel_a, window_function="lag", window_alias="prev_a", children=lag, offset=list(duckdb:::expr_constant(1)))
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    expected_result <- data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4), prev_a=c(NA, 1, 2, 3, 4, 5, 6, 7))
    res <- duckdb:::rel_to_altrep(order_over_window)
    expect_equal(res, expected_result)
})

test_that("Window lead function works as expected", {
#   select a, b, lag(a, 1) OVER () order by a
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    lead <- list(duckdb:::expr_reference("a"))
    window_function <- duckdb:::rel_window(rel=rel_a, window_function="lead", window_alias="next_a", children=lead, offset=list(duckdb:::expr_constant(1)))
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    expected_result <- data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4), next_a=c(2, 3, 4, 5, 6, 7, 8, NA))
    res <- duckdb:::rel_to_altrep(order_over_window)
    expect_equal(res, expected_result)
})

test_that("Window function with string aggregate works", {
#   select j, s, string_agg(s, '|') over (partition by b) from a order by j, s;
    rel_a <- duckdb:::rel_from_df(con, data.frame(r=c(1, 2, 3, 4), a=c("hello", "Big", "world", "42"),b=c(1, 1, 2, 2)))
    str_agg <- list(duckdb:::expr_reference("a"))
    partitions <- list(duckdb:::expr_reference("b"))
    window_function <- duckdb:::rel_window(rel=rel_a, window_function="string_agg", window_alias="str_concat", children=str_agg, partitions=partitions)
    sum2 <- list(duckdb:::expr_reference("r", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    expected_result <- data.frame(r=c(1:4),a=c("hello", "Big", "world", "42"), b=c(1, 1, 2, 2), str_concat=c("hello,Big", "hello,Big", "world,42", "world,42"))
    res <- duckdb:::rel_to_altrep(order_over_window)
    expect_equal(res, expected_result)
})

test_that("You can project from a window function", {
#     select j, i, sum(i) over (partition by j) from a order by 1,2
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1:8),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    sum <- list(duckdb:::expr_reference("a"))
    partitions <- list(duckdb:::expr_reference("b"))
    window_function <- duckdb:::rel_window(rel=rel_a, window_alias="window_result", window_function="sum", children=sum, partitions = partitions)
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    proj <- duckdb:::rapi_rel_project(order_over_window, list(duckdb:::expr_reference("window_result")))
    expected_result <- data.frame(window_result=c(3, 3, 7, 7, 11, 11, 15, 15))
    res = duckdb:::rel_to_altrep(proj)
    expect_equal(res, expected_result)
})

test_that("You can perform window functions on row_number", {
	# select a, b, row_number() OVER () from tmp order by a;
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(8:1),b=c(1, 1, 2, 2, 3, 3, 4, 4)))
    sum <- list(duckdb:::expr_reference("a"))
    partitions <- list(duckdb:::expr_reference("b"))
    window_function <- duckdb:::rel_window(rel=rel_a, window_function="row_number", window_alias="row_number")
    sum2 <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, sum2)
    res <- duckdb:::rel_to_altrep(order_over_window)
    expected_result <- data.frame(a=c(1:8), b=c(4, 4, 3, 3, 2, 2, 1, 1), row_number=(8:1))
    expect_equal(res, expected_result)
})

# also tests order by inside the rank function.
# these tests come from https://dplyr.tidyverse.org/articles/window-functions.html
# in dplyr min_rank = rank
test_that("You can perform the window function min_rank", {
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1, 1, 2, 2, 2)))
    order_by_a <- list(duckdb:::rapi_rel_order(rel_a, list(duckdb:::expr_reference("a"))))
    window_function <- duckdb:::rel_window(rel_a, window_function="rank", window_alias="rank", orders=order_by_a)
	window_res <- duckdb:::rel_to_altrep(window_function)
	expected_result <- data.frame(a=c(1, 1, 2, 2, 2), rank=c(1, 1, 3, 3, 3))
    expect_equal(window_res, expected_result)
})

test_that("You can perform the window function dense_rank", {
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1, 1, 2, 2, 2)))
    order_by_a <- list(duckdb:::rapi_rel_order(rel_a, list(duckdb:::expr_reference("a"))))
    window_function <- duckdb:::rel_window(rel_a, window_function="dense_rank", window_alias="dense_rank", orders=order_by_a)
    order_by_a_output <- list(duckdb:::expr_reference("a", window_function))
    order_over_window <- duckdb:::rapi_rel_order(window_function, order_by_a_output)
    res <- duckdb:::rel_to_altrep(order_over_window)
    expected_result <- data.frame(a=c(1, 1, 2, 2, 2), dense_rank=c(1, 1, 2, 2, 2))
    expect_equal(res, expected_result)
})


test_that("You can perform the window function cume_dist", {
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1, 1, 2, 2, 2)))
    order_by_a <- list(duckdb:::rapi_rel_order(rel_a, list(duckdb:::expr_reference("a"))))
    window_function <- duckdb:::rel_window(rel_a, window_function="cume_dist", window_alias="cume_dist", orders=order_by_a)
    order_over_window <- list(duckdb:::expr_reference("a", window_function))
    result <- duckdb:::rapi_rel_order(window_function, order_over_window)
    res = duckdb:::rel_to_altrep(result)
    expected_result <- data.frame(a=c(1, 1, 2, 2, 2), cume_dist=c(0.4, 0.4, 1.0, 1.0, 1.0))
    expect_equal(res, expected_result)
})

test_that("You can perform the window function percent rank", {
    rel_a <- duckdb:::rel_from_df(con, data.frame(a=c(1, 1, 2, 2, 2)))
    order_by_a <- list(duckdb:::rapi_rel_order(rel_a, list(duckdb:::expr_reference("a"))))
    window_function <- duckdb:::rel_window(rel_a, window_function="percent_rank", window_alias="percent_rank", orders=order_by_a)
    order_over_window <- list(duckdb:::expr_reference("a", window_function))
    result <- duckdb:::rapi_rel_order(window_function, order_over_window)
    res = duckdb:::rel_to_altrep(result)
    expected_result <- data.frame(a=c(1, 1, 2, 2, 2), percent_rank=c(0.0, 0.0, 0.5, 0.5, 0.5))
    expect_equal(res, expected_result)
})

# with and without offsets

