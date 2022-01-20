# helper function to skip tests if we don't have the suggested package
# https://github.com/DyfanJones/noctua/blob/master/tests/testthat/helper.R
skip_if_package_not_available <- function(pkg) {
  if (!nzchar(system.file(package = pkg)))
    skip(sprintf("`%s` not available for testing", pkg))
}

skip_if_no_R4 <- function() {
  if (R.Version()$major<4) {
    skip("R 4.0.0 or newer not available for testing")
  }
}

test_that("dbplyr generic scalars translated correctly", {
  skip_if_no_R4()
  skip_if_package_not_available("dbplyr")
  library(dbplyr)
  translate <- function(...) translate_sql(...,con=translate_duckdb())

  expect_equal(translate(as.character(1)),               sql(r"{CAST(1.0 AS TEXT)}"))
  expect_equal(translate(as.character(1L)),              sql(r"{CAST(1 AS TEXT)}"))
  expect_equal(translate(as.numeric(1)),                 sql(r"{CAST(1.0 AS NUMERIC)}"))
  expect_equal(translate(as.double(1.2)),                sql(r"{CAST(1.2 AS NUMERIC)}"))
  expect_equal(translate(as.integer(1.2)),               sql(r"{CAST(1.2 AS INTEGER)}"))
  expect_equal(translate(as.integer64(1.2)),             sql(r"{CAST(1.2 AS BIGINT)}"))
  expect_equal(translate(as.logical('TRUE')),            sql(r"{CAST('TRUE' AS BOOLEAN)}"))
  expect_equal(translate(tolower('HELLO')),              sql(r"{LOWER('HELLO')}"))
  expect_equal(translate(toupper('hello')),              sql(r"{UPPER('hello')}"))
  expect_equal(translate(pmax(1,2,na.rm=TRUE)),          sql(r"{GREATEST(1.0, 2.0)}"))
  expect_equal(translate(pmin(1,2,na.rm=TRUE)),          sql(r"{LEAST(1.0, 2.0)}"))
  expect_equal(translate(as.character('2020-01-01')),    sql(r"{CAST('2020-01-01' AS TEXT)}"))
  expect_equal(translate(c('2020-01-01', '2020-13-02')), sql(r"{('2020-01-01', '2020-13-02')}"))
  expect_equal(translate(iris[['sepal_length']]),        sql(r"{"iris"."sepal_length"}"))
  expect_equal(translate(iris[[1]]),                     sql(r"{"iris"[1]}"))
  expect_equal(translate(cot(x)),                        sql(r"{COT("x")}"))
  expect_equal(translate(substr('test', 2, 3)),          sql(r"{SUBSTR('test', 2, 2)}"))
})

test_that("duckdb custom scalars translated correctly", {
  skip_if_no_R4()
  skip_if_package_not_available("dbplyr")
  library(dbplyr)
  translate <- function(...) translate_sql(...,con=translate_duckdb())

  #  expect_equal(translate(as(1,"CHARACTER")),            sql(r"{CAST(1.0 AS TEXT}"))        # Not implemented
  expect_equal(translate(as.raw(10)),                    sql(r"{CAST(10.0 AS VARBINARY)}"))
  expect_equal(translate(13 %% 5),                       sql(r"{(CAST((13.0) AS INTEGER)) % (CAST((5.0) AS INTEGER))}"))
  expect_equal(translate(35.8 %/% 4),                    sql(r"{(((CAST((35.8) AS INTEGER)) - (CAST((35.8) AS INTEGER)) % (CAST((4.0) AS INTEGER)))) / (CAST((4.0) AS INTEGER))}"))
  expect_equal(translate(35.8^2.51),                     sql(r"{POW(35.8, 2.51)}"))
  expect_equal(translate(bitwOr(x, 128L)),               sql(r"{(CAST("x" AS INTEGER)) | (CAST(128 AS INTEGER))}"))
  expect_equal(translate(bitwAnd(x, 128)),               sql(r"{(CAST("x" AS INTEGER)) & (CAST(128.0 AS INTEGER))}"))
  expect_equal(translate(bitwXor(x, 128L)),              sql(r"{XOR((CAST("x" AS INTEGER)), (CAST(128 AS INTEGER)))}"))
  expect_equal(translate(bitwNot(x)),                    sql(r"{~(CAST("x" AS INTEGER))}"))
  expect_equal(translate(bitwShiftL(x, 5L)),             sql(r"{(CAST("x" AS INTEGER)) << (CAST(5 AS INTEGER))}"))
  expect_equal(translate(bitwShiftR(x, 4L)),             sql(r"{(CAST("x" AS INTEGER)) >> (CAST(4 AS INTEGER))}"))
  expect_equal(translate(log(x)),                        sql(r"{LN("x")}"))
  expect_equal(translate(log(x,base=5)),                 sql(r"{LOG("x") / LOG(5.0)}"))
  expect_equal(translate(log10(x)),                      sql(r"{LOG10("x")}"))
  expect_equal(translate(log2(x)),                       sql(r"{LOG2("x")}"))
  expect_equal(translate(is.na(var1)),                   sql(r"{("var1" IS NULL OR PRINTF('%f',"var1") = 'nan')}"))
  expect_equal(translate(is.nan(var1)),                  sql(r"{("var1" IS NOT NULL AND PRINTF('%f',"var1") = 'nan')}"))
  expect_equal(translate(is.infinite(var1)),             sql(r"{("var1" IS NOT NULL AND REGEXP_MATCHES(PRINTF('%f',"var1"),'inf'))}"))
  expect_equal(translate(is.finite(var1)),               sql(r"{(NOT ("var1" IS NULL OR REGEXP_MATCHES(PRINTF('%f',"var1"),'inf|nan')))}"))
  expect_equal(translate(grepl('pattern', text)),        sql(r"{REGEXP_MATCHES("text", 'pattern')}"))
  expect_equal(translate(grepl('pattern', text,
                               ignore.case=TRUE)),       sql(r"{REGEXP_MATCHES("text", '(?i)pattern')}"))
  expect_error(translate(grepl('dummy', txt, perl=TRUE)))
  expect_equal(translate(regexpr('pattern', text)),      sql(r"{(LENGTH(LIST_EXTRACT(STRING_SPLIT_REGEX("text", 'pattern'),0))-(LENGTH(LIST_EXTRACT(STRING_SPLIT_REGEX("text", 'pattern'),0))+2)/(LENGTH("text")+2)*(LENGTH("text")+2)+1)}"))
  expect_equal(translate(round(x, digits = 1.1)),        sql(r"{ROUND("x", CAST(ROUND(1.1, 0) AS INTEGER))}"))
  expect_equal(translate(as.Date('2019-01-01')),         sql(r"{CAST('2019-01-01' AS DATE)}"))
  expect_equal(translate(as.POSIXct('2019-01-01 01:01:01')),sql(r"{CAST('2019-01-01 01:01:01' AS TIMESTAMP)}"))

})



test_that("pasting translated correctly", {
  skip_if_no_R4()
  skip_if_package_not_available("dbplyr")
  library(dbplyr)
  translate <- function(...) translate_sql(...,con=translate_duckdb())

  expect_equal(translate(paste("hi","bye")),             sql(r"{CONCAT_WS(' ', 'hi', 'bye')}"))
  expect_equal(translate(paste("hi","bye", sep = "-")),  sql(r"{CONCAT_WS('-', 'hi', 'bye')}"))
  expect_equal(translate(paste0("hi","bye")),            sql(r"{CONCAT_WS('', 'hi', 'bye')}"))

  expect_equal(translate(paste(x, y), window = FALSE),   sql(r"{CONCAT_WS(' ', "x", "y")}"))
  expect_equal(translate(paste0(x, y), window = FALSE),  sql(r"{CONCAT_WS('', "x", "y")}"))

  expect_error(translate(paste0(x, collapse = ""), window = FALSE), "`collapse` not supported")
})


# lubridate functions

test_that("custom lubridate functions translated correctly", {
  skip_if_no_R4()
  skip_if_package_not_available("dbplyr")
  library(dbplyr)
  translate <- function(...) translate_sql(...,con=translate_duckdb())

  expect_equal(translate(yday(x)),                           sql(r"{EXTRACT(DOY FROM "x")}"))
  expect_equal(translate(quarter(x)),                        sql(r"{EXTRACT(QUARTER FROM "x")}"))
  expect_equal(translate(quarter(x, with_year = TRUE)),      sql(r"{(EXTRACT(YEAR FROM "x") || '.' || EXTRACT(QUARTER FROM "x"))}"))
  expect_equal(translate(quarter(x, type = "year.quarter")), sql(r"{(EXTRACT(YEAR FROM "x") || '.' || EXTRACT(QUARTER FROM "x"))}"))
  expect_equal(translate(quarter(x, type="quarter")),        sql(r"{EXTRACT(QUARTER FROM "x")}"))
  expect_equal(translate(quarter(x, type=TRUE)),             sql(r"{(EXTRACT(YEAR FROM "x") || '.' || EXTRACT(QUARTER FROM "x"))}"))
  expect_equal(translate(quarter(x, type=FALSE)),            sql(r"{EXTRACT(QUARTER FROM "x")}"))
  expect_equal(translate(quarter(x, type="date_first")),     sql(r"{(CAST(DATE_TRUNC('QUARTER', "x") AS DATE))}"))
  expect_equal(translate(quarter(x, type="date_last")),      sql(r"{(CAST((DATE_TRUNC('QUARTER',"x") + INTERVAL '1 QUARTER' - INTERVAL '1 DAY') AS DATE))}"))
  expect_error(translate(quarter(x, type="other")))
  expect_error(translate(quarter(x, fiscal_start = 2)))
  expect_equal(translate(month(x, label=FALSE)),             sql(r"{EXTRACT(MONTH FROM "x")}"))
  expect_equal(translate(month(x, label=TRUE)),              sql(r"{STRFTIME("x", '%b')}"))
  expect_equal(translate(month(x, label=TRUE, abbr=FALSE)),  sql(r"{STRFTIME("x", '%B')}"))
  expect_equal(translate(qday(x)),                           sql(r"{DATE_DIFF('DAYS',DATE_TRUNC('QUARTER',CAST(("x") AS DATE)),(CAST(("x") AS DATE) + INTERVAL '1 DAY'))}"))
  expect_equal(translate(wday(x)),                           sql(r"{EXTRACT('dow' FROM CAST("x" AS DATE) + 0) + 1}"))
  expect_equal(translate(wday(x,week_start=4)),              sql(r"{EXTRACT('dow' FROM CAST("x" AS DATE) + 3) + 1}"))
  expect_equal(translate(wday(x,label=TRUE)),                sql(r"{STRFTIME("x", '%a')}"))
  expect_equal(translate(wday(x,label=TRUE, abbr=FALSE)),    sql(r"{STRFTIME("x", '%A')}"))
  expect_equal(translate(seconds(x)),                        sql(r"{TO_SECONDS(CAST("x" AS BIGINT))}"))
  expect_equal(translate(minutes(x)),                        sql(r"{TO_MINUTES(CAST("x" AS BIGINT))}"))
  expect_equal(translate(hours(x)),                          sql(r"{TO_HOURS(CAST("x" AS BIGINT))}"))
  expect_equal(translate(days(x)),                           sql(r"{TO_DAYS(CAST("x" AS INTEGER))}"))
  expect_equal(translate(weeks(x)),                          sql(r"{TO_DAYS(7 * CAST("x" AS INTEGER))}"))
  expect_equal(translate(months(x)),                         sql(r"{TO_MONTHS(CAST("x" AS INTEGER))}"))
  expect_equal(translate(years(x)),                          sql(r"{TO_YEARS(CAST("x" AS INTEGER))}"))
  expect_equal(translate(floor_date(x, 'month')),            sql(r"{DATE_TRUNC('month', "x")}"))
  expect_equal(translate(floor_date(x, 'week')),             sql(r"{DATE_TRUNC('week', "x")}"))
})

# stringr functions

test_that("custom stringr functions translated correctly", {
  skip_if_no_R4()
  skip_if_package_not_available("dbplyr")
  library(dbplyr)
  translate <- function(...) translate_sql(...,con=translate_duckdb())

  expect_equal(translate(str_c(x, y)),                       sql(r"{CONCAT_WS('', "x", "y")}"))
  expect_error(translate(str_c(x, collapse = "")), "`collapse` not supported")
  expect_equal(translate(str_detect(x, y)),                  sql(r"{REGEXP_MATCHES("x", "y")}"))
  expect_equal(translate(str_detect(x, y, negate = TRUE)),   sql(r"{(NOT(REGEXP_MATCHES("x", "y")))}"))
  expect_equal(translate(str_replace(x, y, z)),              sql(r"{REGEXP_REPLACE("x", "y", "z")}"))
  expect_equal(translate(str_replace_all(x, y, z)),          sql(r"{REGEXP_REPLACE("x", "y", "z", 'g')}"))
  expect_equal(translate(str_squish(x)),                     sql(r"{TRIM(REGEXP_REPLACE("x", '\s+', ' ', 'g'))}"))
  expect_equal(translate(str_remove(x, y)),                  sql(r"{REGEXP_REPLACE("x", "y", '')}"))
  expect_equal(translate(str_remove_all(x, y)),              sql(r"{REGEXP_REPLACE("x", "y", '', 'g')}"))
  expect_equal(translate(str_to_title(x)),                   sql(r"{(UPPER("x"[0]) || "x"[1:NULL])}"))
  expect_equal(translate(str_starts(x, y)),                  sql(r"{REGEXP_MATCHES("x",'^'||"y")}"))
  expect_equal(translate(str_ends(x, y)),                    sql(r"{REGEXP_MATCHES("x","y"||'$')}"))
  expect_equal(translate(str_pad(x,width=10)),               sql(r"{LPAD("x", 10, ' ')}"))
  expect_equal(translate(str_pad(x,width=10,side="right")),  sql(r"{RPAD("x", 10, ' ')}"))
  expect_equal(translate(str_pad(x,width=10,side="both",pad="<")),  sql(r"{RPAD(REPEAT('<', (10 - LENGTH("x")) / 2) || "x", 10, '<')}"))
  expect_error(translate(str_pad(x,width=10,side="other")))
})

test_that("datetime escaping working as in DBI", {
  skip_if_no_R4()
  skip_if_package_not_available("dbplyr")
  library(dbplyr)
  con=translate_duckdb()

  test_date <- as.Date("2020-01-01")
  expect_equal(escape(test_date, con = con),                 sql(r"{'2020-01-01'::date}"))
  expect_equal(escape("2020-01-01", con = con),              sql(r"{'2020-01-01'}"))

  test_datetime <- as.POSIXct("2020-01-01 01:23:45 UTC",tz="UTC")
  expect_equal(escape(test_datetime, con = con),             sql(r"{'2020-01-01 01:23:45'::timestamp}"))
  expect_equal(escape("2020-01-01 01:23:45 UTC", con = con), sql(r"{'2020-01-01 01:23:45 UTC'}"))

  test_datetime_tz <- as.POSIXct("2020-01-01 18:23:45 UTC",tz="America/Los_Angeles")
  expect_equal(escape(test_datetime_tz, con = con),          sql(r"{'2020-01-02 02:23:45'::timestamp}"))
  expect_equal(escape("2020-01-01 18:23:45 PST", con = con), sql(r"{'2020-01-01 18:23:45 PST'}"))
})

test_that("two variable aggregates are translated correctly", {
  skip_if_no_R4()
  skip_if_package_not_available("dbplyr")
  library(dbplyr)
  translate <- function(...) translate_sql(...,con=translate_duckdb())

  expect_equal(translate(cor(x, y), window = FALSE),         sql(r"{CORR("x", "y")}"))
  expect_equal(translate(cor(x, y), window = TRUE),          sql(r"{CORR("x", "y") OVER ()}"))
})
