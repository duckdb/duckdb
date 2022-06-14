#' DuckDB SQL backend for dbplyr
#'
#' @description
#' This is a SQL backend for dbplyr tailored to take into account DuckDB's
#' possibilities. This mainly follows the backend for PostgreSQL, but
#' contains more mapped functions.
#'
#' @name backend-duckdb
#' @aliases NULL
#' @examples
#' library(dplyr, warn.conflicts = FALSE)
#' con <- DBI::dbConnect(duckdb::duckdb(), path = ":memory:")
#'
#' dbiris <- copy_to(con, iris, overwrite = TRUE)
#'
#' dbiris %>% select(Petal.Length, Petal.Width) %>% filter(Petal.Length > 1.5) %>% head(5)
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
NULL

#' Connection object for simulation of the SQL generation without actual database.
#' dbplyr overrides database specific identifier and string quotes
#' @param ... Any parameters to be forwarded
#' @export
#' @rdname backend-duckdb
simulate_duckdb <- function(...) {
  structure(list(), ..., class = c("duckdb_connection", "TestConnection", "DBIConnection"))
}

#' Connection object for simulation of the SQL generation without actual database.
#' This version keeps the database specific identifier and string quotes, i.e.
#' allows to translate to DuckDB SQL dialect.
#' @param ... Any parameters to be forwarded
#' @export
#' @rdname backend-duckdb
translate_duckdb <- function(...) {
  structure(list(), ..., class = c("duckdb_connection", "DBIConnection"))
}

# Declare which version of dbplyr API is being called.
# @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
# @name dbplyr_edition
dbplyr_edition.duckdb_connection <- function(con) {
  2L
}

# Description of the database connection
# @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
# @name db_connection_describe
# @return
# String consisting of DuckDB version, user login name, operating system, R version and the name of database
db_connection_describe.duckdb_connection <- function(con) {
  info <- DBI::dbGetInfo(con)
  paste0(
    "DuckDB ", info$db.version, " [", Sys.info()["login"], "@",
    paste(Sys.info()[c("sysname", "release")], collapse = " "), ":",
    "R ", R.version$major, ".", R.version$minor, "/", info$dbname, "]"
  )
}

duckdb_grepl <- function(pattern, x, ignore.case = FALSE, perl = FALSE, fixed = FALSE, useBytes = FALSE) {
  # https://duckdb.org/docs/sql/functions/patternmatching
  if (any(c(perl, fixed, useBytes))) {
    stop("Parameters `perl`, `fixed` and `useBytes` in grepl are not currently supported in DuckDB backend", call. = FALSE)
  }

  sql_expr <- pkg_method("sql_expr", "dbplyr")

  if (ignore.case) {
    icpattern <- paste0("(?i)", pattern)
    sql_expr(REGEXP_MATCHES((!!x), (!!icpattern)))
  } else {
    sql_expr(REGEXP_MATCHES((!!x), (!!pattern)))
  }
}


# Customized translation functions for DuckDB SQL
# @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
# @name sql_translation
sql_translation.duckdb_connection <- function(con) {
  sql_variant <- pkg_method("sql_variant", "dbplyr")
  sql_translator <- pkg_method("sql_translator", "dbplyr")
  sql <- pkg_method("sql", "dbplyr")
  build_sql <- pkg_method("build_sql", "dbplyr")
  sql_expr <- pkg_method("sql_expr", "dbplyr")
  sql_prefix <- pkg_method("sql_prefix", "dbplyr")
  sql_cast <- pkg_method("sql_cast", "dbplyr")
  sql_paste <- pkg_method("sql_paste", "dbplyr")
  sql_aggregate <- pkg_method("sql_aggregate", "dbplyr")
  sql_aggregate_2 <- pkg_method("sql_aggregate_2", "dbplyr")
  win_aggregate <- pkg_method("win_aggregate", "dbplyr")
  win_aggregate_2 <- pkg_method("win_aggregate_2", "dbplyr")
  win_over <- pkg_method("win_over", "dbplyr")
  win_current_order <- pkg_method("win_current_order", "dbplyr")
  win_current_group <- pkg_method("win_current_order", "dbplyr")


  base_scalar <- pkg_method("base_scalar", "dbplyr")
  base_agg <- pkg_method("base_agg", "dbplyr")
  base_win <- pkg_method("base_win", "dbplyr")

  sql_variant(
    sql_translator(
      .parent = base_scalar,
      as.raw = sql_cast("VARBINARY"),
      `%%` = function(a, b) sql_expr(FMOD(!!a, !!b)),
      `%/%` = function(a, b) sql_expr(FDIV(!!a, !!b)),
      `^` = sql_prefix("POW", 2),
      bitwOr = function(a, b) sql_expr((CAST((!!a) %AS% INTEGER)) | (CAST((!!b) %AS% INTEGER))),
      bitwAnd = function(a, b) sql_expr((CAST((!!a) %AS% INTEGER)) & (CAST((!!b) %AS% INTEGER))),
      bitwXor = function(a, b) sql_expr(XOR((CAST((!!a) %AS% INTEGER)), (CAST((!!b) %AS% INTEGER)))),
      bitwNot = function(a) sql_expr(~ (CAST((!!a) %AS% INTEGER))),
      bitwShiftL = function(a, b) sql_expr((CAST((!!a) %AS% INTEGER)) %<<% (CAST((!!b) %AS% INTEGER))),
      bitwShiftR = function(a, b) sql_expr((CAST((!!a) %AS% INTEGER)) %>>% (CAST((!!b) %AS% INTEGER))),
      log = function(x, base = exp(1)) {
        if (isTRUE(all.equal(base, exp(1)))) {
          sql_expr(LN(!!x))
        } else
        if (base == 10) {
          sql_expr(LOG10(!!x))
        } else
        if (base == 2) {
          sql_expr(LOG2(!!x))
        } else {
          sql_expr(LOG(!!x) / LOG(!!base))
        }
      },
      log10 = sql_prefix("LOG10", 1),
      log2 = sql_prefix("LOG2", 1),

      # See https://github.com/duckdb/duckdb/issues/530 about NaN, infinites and NULL in DuckDB
      # The following is how R functions for detecting those should behave:
      # Function 	    Inf 	–Inf 	NaN 	NA
      # is.finite() 	FALSE FALSE FALSE FALSE
      # is.infinite() TRUE 	TRUE 	FALSE FALSE
      # is.nan() 	    FALSE FALSE TRUE 	FALSE
      # is.na() 	    FALSE FALSE TRUE 	TRUE
      # https://github.com/duckdb/duckdb/issues/3019
      #      is.na = function(a) build_sql("(", a, " IS NULL OR PRINTF('%f', ", a, ") = 'nan')"),
      is.nan = function(a) build_sql("(", a, " IS NOT NULL AND PRINTF('%f', ", a, ") = 'nan')"),
      is.infinite = function(a) build_sql("(", a, " IS NOT NULL AND REGEXP_MATCHES(PRINTF('%f', ", a, "), 'inf'))"),
      is.finite = function(a) build_sql("(NOT (", a, " IS NULL OR REGEXP_MATCHES(PRINTF('%f', ", a, "), 'inf|nan')))"),
      grepl = duckdb_grepl,

      # Return index where the first match starts,-1 if no match
      regexpr = function(p, x) {
        build_sql("(CASE WHEN REGEXP_MATCHES(", x, ", ", p, ") THEN (LENGTH(LIST_EXTRACT(STRING_SPLIT_REGEX(", x, ", ", p, "), 0))+1) ELSE -1 END)")
      },
      round = function(x, digits) sql_expr(ROUND(!!x, CAST(ROUND((!!digits), 0L) %AS% INTEGER))),
      as.Date = sql_cast("DATE"),
      as.POSIXct = sql_cast("TIMESTAMP"),

      # lubridate functions

      month = function(x, label = FALSE, abbr = TRUE) {
        if (!label) {
          sql_expr(EXTRACT(MONTH %FROM% !!x))
        } else {
          if (abbr) {
            sql_expr(STRFTIME(!!x, "%b"))
          } else {
            sql_expr(STRFTIME(!!x, "%B"))
          }
        }
      },
      quarter = function(x, type = "quarter", fiscal_start = 1, with_year = identical(type, "year.quarter")) {
        if (fiscal_start != 1) {
          stop("`fiscal_start` is not yet supported in DuckDB translation. Must be 1.", call. = FALSE)
        }
        if (is.logical(type)) {
          type <- if (type) {
            "year.quarter"
          } else {
            "quarter"
          }
        }
        if (with_year) {
          type <- "year.quarter"
        }
        switch(type,
          quarter = {
            sql_expr(EXTRACT(QUARTER %FROM% !!x))
          },
          year.quarter = {
            sql_expr((EXTRACT(YEAR %FROM% !!x) || "." || EXTRACT(QUARTER %FROM% !!x)))
          },
          date_first = {
            sql_expr((CAST(DATE_TRUNC("QUARTER", !!x) %AS% DATE)))
          },
          date_last = {
            sql_expr((CAST((DATE_TRUNC("QUARTER", !!x) + !!sql("INTERVAL '1 QUARTER'") - !!sql("INTERVAL '1 DAY'")) %AS% DATE)))
          },
          stop(paste("Unsupported type", type), call. = FALSE)
        )
      },
      qday = function(x) {
        build_sql("DATE_DIFF('DAYS', DATE_TRUNC('QUARTER', CAST((", x, ") AS DATE)), (CAST((", x, ") AS DATE) + INTERVAL '1 DAY'))")
      },
      wday = function(x, label = FALSE, abbr = TRUE, week_start = NULL) {
        if (!label) {
          week_start <- if (!is.null(week_start)) week_start else getOption("lubridate.week.start", 7)
          offset <- as.integer(7 - week_start)
          sql_expr(EXTRACT("dow" %FROM% CAST((!!x) %AS% DATE) + !!offset) + 1L)
        } else if (label && !abbr) {
          sql_expr(STRFTIME(!!x, "%A"))
        } else if (label && abbr) {
          sql_expr(STRFTIME(!!x, "%a"))
        } else {
          stop("Unrecognized arguments to `wday`", call. = FALSE)
        }
      },
      yday = function(x) sql_expr(EXTRACT(DOY %FROM% !!x)),

      # These work fine internally, but getting INTERVAL-type data out of DuckDB
      # seems problematic until there is a fix for the issue #1920 / #2900
      # (https://github.com/duckdb/duckdb/issues/1920)
      seconds = function(x) {
        sql_expr(TO_SECONDS(CAST((!!x) %AS% BIGINT)))
      },
      minutes = function(x) {
        sql_expr(TO_MINUTES(CAST((!!x) %AS% BIGINT)))
      },
      hours = function(x) {
        sql_expr(TO_HOURS(CAST((!!x) %AS% BIGINT)))
      },
      days = function(x) {
        sql_expr(TO_DAYS(CAST((!!x) %AS% INTEGER)))
      },
      weeks = function(x) {
        sql_expr(TO_DAYS(7L * CAST((!!x) %AS% INTEGER)))
      },
      months = function(x) {
        sql_expr(TO_MONTHS(CAST((!!x) %AS% INTEGER)))
      },
      years = function(x) {
        sql_expr(TO_YEARS(CAST((!!x) %AS% INTEGER)))
      },

      # Week_start algorithm: https://github.com/tidyverse/lubridate/issues/509#issuecomment-287030620
      floor_date = function(x, unit = "seconds", week_start = NULL) {
        if (unit %in% c("week", "weeks")) {
          week_start <- if (!is.null(week_start)) week_start else getOption("lubridate.week.start", 7)
          if (week_start == 1) {
            sql_expr(DATE_TRUNC(!!unit, !!x))
          } else {
            offset <- as.integer(7 - week_start)
            sql_expr(CAST((!!x) %AS% DATE) - CAST(EXTRACT("dow" %FROM% CAST((!!x) %AS% DATE) + !!offset) %AS% INTEGER))
          }
        } else {
          sql_expr(DATE_TRUNC(!!unit, !!x))
        }
      },
      paste = sql_paste(" "),
      paste0 = sql_paste(""),

      # stringr functions
      str_c = sql_paste(""),
      str_detect = function(string, pattern, negate = FALSE) {
        if (negate) {
          sql_expr((NOT(REGEXP_MATCHES(!!string, !!pattern))))
        } else {
          sql_expr(REGEXP_MATCHES(!!string, !!pattern))
        }
      },
      str_replace = function(string, pattern, replacement) {
        sql_expr(REGEXP_REPLACE(!!string, !!pattern, !!replacement))
      },
      str_replace_all = function(string, pattern, replacement) {
        sql_expr(REGEXP_REPLACE(!!string, !!pattern, !!replacement, "g"))
      },
      str_squish = function(string) {
        sql_expr(TRIM(REGEXP_REPLACE(!!string, "\\s+", " ", "g")))
      },
      str_remove = function(string, pattern) {
        sql_expr(REGEXP_REPLACE(!!string, !!pattern, ""))
      },
      str_remove_all = function(string, pattern) {
        sql_expr(REGEXP_REPLACE(!!string, !!pattern, "", "g"))
      },
      #      str_to_title = function(string) {
      #        sql_expr(INITCAP(!!string))
      #      },
      str_to_sentence = function(string) {
        build_sql("(UPPER(", string, "[0]) || ", string, "[1:NULL])")
      },
      # Respect OR (|) operator: https://github.com/tidyverse/stringr/pull/340
      str_starts = function(string, pattern) {
        build_sql("REGEXP_MATCHES(", string, ",'^(?:'||", pattern, "))")
      },
      str_ends = function(string, pattern) {
        build_sql("REGEXP_MATCHES((?:", string, ",", pattern, "||')$')")
      },
      # NOTE: GREATEST needed because DuckDB PAD-functions truncate the string if width < length of string
      str_pad = function(string, width, side = "left", pad = " ", use_length = FALSE) {
        if (side %in% c("left")) {
          sql_expr(LPAD(!!string, CAST(GREATEST(!!as.integer(width), LENGTH(!!string)) %AS% INTEGER), !!pad))
        } else if (side %in% c("right")) {
          sql_expr(RPAD(!!string, CAST(GREATEST(!!as.integer(width), LENGTH(!!string)) %AS% INTEGER), !!pad))
        } else if (side %in% c("both")) {
          sql_expr(RPAD(REPEAT(!!pad, (!!as.integer(width) - LENGTH(!!string)) / 2L) %||% !!string, CAST(GREATEST(!!as.integer(width), LENGTH(!!string)) %AS% INTEGER), !!pad))
        } else {
          stop('Argument \'side\' should be "left", "right" or "both"', call. = FALSE)
        }
      }
    ),
    sql_translator(
      .parent = base_agg,
      cor = sql_aggregate_2("CORR"),
      cov = sql_aggregate_2("COVAR_SAMP"),
      sd = sql_aggregate("STDDEV", "sd"),
      var = sql_aggregate("VARIANCE", "var"),
      all = sql_aggregate("BOOL_AND", "all"),
      any = sql_aggregate("BOOL_OR", "any"),
      str_flatten = function(x, collapse) sql_expr(STRING_AGG(!!x, !!collapse)),
      first = sql_prefix("FIRST", 1),
      last = sql_prefix("LAST", 1)
    ),
    sql_translator(
      .parent = base_win,
      cor = win_aggregate_2("CORR"),
      cov = win_aggregate_2("COVAR_SAMP"),
      sd = win_aggregate("STDDEV"),
      var = win_aggregate("VARIANCE"),
      all = win_aggregate("BOOL_AND"),
      any = win_aggregate("BOOL_OR"),
      str_flatten = function(x, collapse) {
        win_over(
          sql_expr(STRING_AGG(!!x, !!collapse)),
          partition = win_current_group(),
          order = win_current_order()
        )
      }
    )
  )
}


# Customized translation for comparing to objects in DuckDB SQL
# @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
# @param x First object to be compared
# @param y Second object to be compared
# @name sql_expr_matches
sql_expr_matches.duckdb_connection <- function(con, x, y) {
  build_sql <- pkg_method("build_sql", "dbplyr")
  # https://duckdb.org/docs/sql/expressions/comparison_operators
  build_sql(x, " IS NOT DISTINCT FROM ", y, con = con)
}

# Customized escape translation for date objects
# @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
# @param x Date object to be escaped
# @name sql_escape_date
sql_escape_date.duckdb_connection <- function(con, x) {
  # https://github.com/tidyverse/dbplyr/issues/727
  dbQLit <- pkg_method("dbQuoteLiteral", "DBI")
  dbQLit(con, x)
}

# Customized escape translation for datetime objects
# @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
# @param x Datetime object to be escaped
# @name sql_escape_datetime
sql_escape_datetime.duckdb_connection <- function(con, x) {
  dbQLit <- pkg_method("dbQuoteLiteral", "DBI")
  dbQLit(con, x)
}

# Customized translation for fill function
# @param .con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
# @param .data Data frame
# @param cols_to_fill Which colums to be altered
# @param order_by_cols Defined order of variables
# @param .direction Direction in which to fill missing values.
# @name dbplyr_fill0
dbplyr_fill0.duckdb_connection <- function(.con, .data, cols_to_fill, order_by_cols, .direction) {
  dbplyr_fill0 <- pkg_method("dbplyr_fill0.SQLiteConnection", "dbplyr")

  # Required because of the bug in dbplyr (con is not passed to "translate_sql(cumsum..." call)
  # See https://github.com/tidyverse/dbplyr/pull/753
  setcon <- pkg_method("set_current_con", "dbplyr")
  setcon(.con)

  dbplyr_fill0(.con, .data, cols_to_fill, order_by_cols, .direction)
}

# Customized handling for tbl() to allow the use of replacement scans
# @param src .con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
# @param from Table or parquet/csv -files to be registered
# @param cache Enable object cache for parquet files
tbl.duckdb_connection <- function(src, from, cache = FALSE, ...) {
  ident_q <- pkg_method("ident_q", "dbplyr")
  if (!DBI::dbExistsTable(src, from)) from <- ident_q(from)
  if (cache) DBI::dbExecute(src, "PRAGMA enable_object_cache")
  NextMethod("tbl")
}

# Needed to suppress the R CHECK notes (due to the use of sql_expr)
globalVariables(c("REGEXP_MATCHES", "CAST", "%AS%", "INTEGER", "XOR", "%<<%", "%>>%", "LN", "LOG", "ROUND", "EXTRACT", "%FROM%", "MONTH", "STRFTIME", "QUARTER", "YEAR", "DATE_TRUNC", "DATE", "DOY", "TO_SECONDS", "BIGINT", "TO_MINUTES", "TO_HOURS", "TO_DAYS", "TO_WEEKS", "TO_MONTHS", "TO_YEARS", "STRPOS", "NOT", "REGEXP_REPLACE", "TRIM", "LPAD", "RPAD", "%||%", "REPEAT", "LENGTH", "STRING_AGG", "GREATEST", "LIST_EXTRACT", "LOG10", "LOG2", "STRING_SPLIT_REGEX", "FLOOR", "FMOD", "FDIV"))
