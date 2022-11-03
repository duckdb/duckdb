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
  expect_equivalent(mtcars, as.data.frame(rel_from_df(con, mtcars)))
  expect_equivalent(iris, as.data.frame(rel_from_df(con, iris)))
})


test_that("we can create various expressions and don't crash", {
  ref <- expr_reference('asdf')
  print(ref)
  expect_error(expr_reference(NA))
#  expect_error(expr_reference(as.character(NA)))
  expect_error(expr_reference(''))
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
    gen_rand_string <- function(x, max_len) paste0(chars[sample(1:length(chars), runif(1)*max_len, replace=TRUE)], collapse="")

    test_string_vec <- c(vapply(1:n, gen_rand_string, "character", max_len), NA, NA, NA, NA, NA, NA, NA, NA) # batman

    df <- data.frame(s=test_string_vec, stringsAsFactors=FALSE)
    expect_equivalent(df, as.data.frame(rel_from_df(con, df)))

    res <- rel_from_df(con, df) |> rel_sql("SELECT s::string FROM _")
    expect_equivalent(df, res)

    res <- rel_from_df(con, df) |> rel_sql("SELECT COUNT(*) c FROM _")
    expect_equal(nrow(df), res$c)

    # many rounds yay
    df2 <- df
    for (i in 1:10) {
        df2 <- as.data.frame(rel_from_df(con, df2))
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