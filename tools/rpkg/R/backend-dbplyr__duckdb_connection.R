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
#' x <- tibble(txt=c("why", "video", "cross", "extra", "deal", "authority"))
#' dbx <- copy_to(con,x,overwrite=TRUE)
#'
#' x %>% filter(grepl("^.[hrx]",txt))
#' dbx %>% filter(grepl("^.[hrx]",txt))
#'
#' x %>% mutate(a=stringr::str_pad(txt,10,side="both",pad=">"))
#' dbx %>% mutate(a=str_pad(txt,10,side="both",pad=">"))
#'
#' x %>% mutate(a=stringr::str_replace_all(txt,"[aeiou]","?"))
#' dbx %>% mutate(a=str_replace_all(txt,"[aeiou]","?"))
#'
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' duckdb::duckdb_shutdown(duckdb::duckdb())
NULL

#' Declare which version of dbplyr API is being called.
#' @export
dbplyr_edition.duckdb_connection <- function(con) {
  2L
}

#' Description of the database connection
#'
#' @param con A \code{\link{dbConnect}} object, as returned by \code{dbConnect()}
#' @name db_connection_describe
#' @return
#' String consisting of DuckDB version, user login name, operating system, R version and the name of database
#' @export
db_connection_describe.duckdb_connection <- function(con) {
  info <- DBI::dbGetInfo(con)
  paste0("DuckDB ", info$db.version, " [", Sys.info()["login"], "@",
         paste(Sys.info()[c("sysname","release")],collapse=" "), ":",
         "R ",R.version$major,".",R.version$minor, "/", info$dbname, "]")
}

duckdb_grepl <- function(pattern, x, ignore.case = FALSE, perl = FALSE, fixed = FALSE, useBytes = FALSE) {
  # https://duckdb.org/docs/sql/functions/patternmatching
  if (any(c(perl, fixed, useBytes))) {
    stop("Parameters `perl`, `fixed` and `useBytes` in grepl are not currently supported in DuckDB backend",.call=FALSE)
  }

  sql_expr <- pkg_method("sql_expr","dbplyr")

  if (ignore.case) {
    icpattern <- paste0("(?i)",pattern)
    sql_expr(REGEXP_MATCHES((!!x),(!!icpattern)))
  } else {
    sql_expr(REGEXP_MATCHES((!!x),(!!pattern)))
  }
}

#' @export
sql_translation.duckdb_connection <- function(con) {

  sql_variant <- pkg_method("sql_variant","dbplyr")
  sql_translator <- pkg_method("sql_translator","dbplyr")
  build_sql <- pkg_method('build_sql', "dbplyr")
  sql_expr <- pkg_method("sql_expr","dbplyr")
  sql_prefix <- pkg_method("sql_prefix","dbplyr")
  sql_cast <- pkg_method("sql_cast","dbplyr")
  sql_paste <- pkg_method("sql_paste","dbplyr")
  sql_aggregate <- pkg_method("sql_aggregate","dbplyr")
  sql_aggregate_2 <- pkg_method("sql_aggregate_2","dbplyr")
  win_aggregate <- pkg_method("win_aggregate","dbplyr")
  win_aggregate_2 <- pkg_method("win_aggregate_2","dbplyr")
  win_over <- pkg_method("win_over","dbplyr")
  win_current_order <- pkg_method("win_current_order","dbplyr")
  win_current_group <- pkg_method("win_current_order","dbplyr")

  base_scalar <- pkg_method("base_scalar","dbplyr")
  base_agg <- pkg_method("base_agg","dbplyr")
  base_win <- pkg_method("base_win","dbplyr")

  sql_variant(
    sql_translator(.parent = base_scalar,
                   `%%` = function(a,b) build_sql("(CAST((",a,") AS INTEGER)) % (CAST((",b,") AS INTEGER))"),
                   `%/%` = function(a,b) build_sql("(((CAST((",a,") AS INTEGER)) - (CAST((",a,") AS INTEGER)) % (CAST((",b,") AS INTEGER)))) / (CAST((",b,") AS INTEGER))"),
                   `^` = sql_prefix("POW", 2),
                   bitwOr = function(a,b) sql_expr((CAST((!!a) %AS% INTEGER)) | (CAST((!!b) %AS% INTEGER))),
                   bitwAnd = function(a,b) sql_expr((CAST((!!a) %AS% INTEGER)) & (CAST((!!b) %AS% INTEGER))),
                   bitwXor = function(a,b) sql_expr(XOR((CAST((!!a) %AS% INTEGER)),(CAST((!!b) %AS% INTEGER)))),
                   bitwNot = function(a) sql_expr(~ (CAST((!!a) %AS% INTEGER))),
                   bitwShiftL = function(a,b) sql_expr((CAST((!!a) %AS% INTEGER)) %<<% (CAST((!!b) %AS% INTEGER))),
                   bitwShiftR = function(a,b) sql_expr((CAST((!!a) %AS% INTEGER)) %>>% (CAST((!!b) %AS% INTEGER))),
                   log = function(x, base = exp(1)) {
                     if (isTRUE(all.equal(base, exp(1)))) {
                       sql_expr(LN(!!x))
                     } else {
                       sql_expr(LOG(!!x)/LOG(!!base))
                     }
                   },
                   log10   = sql_prefix("LOG10", 1),
                   log2   = sql_prefix("LOG2", 1),


                   grepl  = duckdb_grepl,
                   round = function(x, digits) sql_expr(ROUND(!!x, CAST(ROUND((!!digits),0L) %AS% INTEGER))),

                   as.Date = sql_cast("DATE"),
                   as.POSIXct = sql_cast("TIMESTAMP"),

                   # lubridate functions

                   month = function(x, label = FALSE, abbr = TRUE) {
                     if (!label) {
                       sql_expr(EXTRACT(MONTH %FROM% !!x))
                     } else {
                       if (abbr) {
                         sql_expr(STRFTIME(!!x,"%b"))
                       } else {
                         sql_expr(STRFTIME(!!x,"%B"))
                       }
                     }
                   },
                   quarter = function(x, type = "quarter", fiscal_start = 1) {
                     if (fiscal_start != 1) {
                       stop("`fiscal_start` is not yet supported in DuckDB translation. Must be 1.", call. = FALSE)
                     }
                     switch(type,
                            quarter={sql_expr(EXTRACT(QUARTER %FROM% !!x))},
                            year.quarter={sql_expr((EXTRACT(YEAR %FROM% !!x) || '.' || EXTRACT(QUARTER %FROM% !!x)))},
                            date_first={sql_expr((CAST(DATE_TRUNC('QUARTER',!!x) %AS% DATE)))},
                            date_last={build_sql("(CAST((DATE_TRUNC('QUARTER',",x,") + INTERVAL '1 QUARTER' - INTERVAL '1 DAY') AS DATE))")},
                            stop(paste("Unsupported type",type), call. = FALSE)
                     )
                   },
                   qday = function(x) { build_sql("DATE_DIFF('DAYS',DATE_TRUNC('QUARTER',CAST((",x,") AS DATE)),(CAST((",x,") AS DATE) + INTERVAL '1 DAY'))") },
                   wday = function(x, label = FALSE, abbr = TRUE, week_start = NULL) {
                     if (!label) {
                       week_start <- if (!is.null(week_start)) week_start else getOption("lubridate.week.start", 7)
                       offset <- as.integer(7 - week_start)
                       sql_expr(EXTRACT("dow" %FROM% CAST((!!x) %AS% DATE) + !!offset) + 1)
                     } else if (label && !abbr) {
                       sql_expr(STRFTIME(!!x,"%A"))
                     } else if (label && abbr) {
                       sql_expr(STRFTIME(!!x,"%a"))
                     } else {
                       stop("Unrecognized arguments to `wday`", call. = FALSE)
                     }
                   },
                   yday = function(x) sql_expr(EXTRACT(DOY %FROM% !!x)),

                   # These require some tweaking or fix for the issue #2900 (https://github.com/duckdb/duckdb/issues/2900)
                   seconds = function(x) { sql_expr(TO_SECONDS(CAST((!!x) %AS% BIGINT))) },
                   minutes = function(x) { sql_expr(TO_MINUTES(CAST((!!x) %AS% BIGINT))) },
                   hours = function(x) { sql_expr(TO_HOURS(CAST((!!x) %AS% BIGINT))) },
                   days = function(x) { sql_expr(TO_DAYS(CAST((!!x) %AS% BIGINT))) },
                   weeks = function(x) { sql_expr(TO_WEEKS(CAST((!!x) %AS% BIGINT))) },
                   months = function(x) { sql_expr(TO_MONTHS(CAST((!!x) %AS% BIGINT))) },
                   years = function(x) { sql_expr(TO_YEARS(CAST((!!x) %AS% BIGINT))) },

                   # Type may change from date to timestamp here now that is not consistent with the R-function
                   # Week_start not implemented and default is different from R-function (1 [Monday] instead of 7 [Sunday])
                   floor_date = function(x, unit = "seconds") {
                     sql_expr(DATE_TRUNC(!!unit, !!x))
                   },

                   paste  = sql_paste(" "),
                   paste0 = sql_paste(""),

                   # stringr functions
                   str_c = sql_paste(""),
                   str_locate  = function(string, pattern) {
                     sql_expr(STRPOS(!!string, !!pattern))
                   },
                   str_detect = function(string, pattern, negate = FALSE) {
                     if (negate==TRUE) {
                       sql_expr((NOT(REGEXP_MATCHES(!!string,!!pattern))))
                     } else {
                       sql_expr(REGEXP_MATCHES(!!string,!!pattern))
                     }
                   },
                   str_replace = function(string, pattern, replacement){
                     sql_expr(REGEXP_REPLACE(!!string, !!pattern, !!replacement))
                   },
                   str_replace_all = function(string, pattern, replacement){
                     sql_expr(REGEXP_REPLACE(!!string, !!pattern, !!replacement, 'g'))
                   },
                   str_squish = function(string){
                     sql_expr(TRIM(REGEXP_REPLACE(!!string, '\\s+', ' ', 'g')))
                   },
                   str_remove = function(string, pattern){
                     sql_expr(REGEXP_REPLACE(!!string, !!pattern, ''))
                   },
                   str_remove_all = function(string, pattern){
                     sql_expr(REGEXP_REPLACE(!!string, !!pattern, '', 'g'))
                   },
                   str_to_title = function(string, pattern){
                     build_sql("(UPPER(",string,"[0]) || ",string,"[1:NULL])")
                   },
                   str_starts = function(string, pattern){
                     build_sql("REGEXP_MATCHES(",string,",'^'||",pattern,")")
                   },
                   str_ends = function(string, pattern){
                     build_sql("REGEXP_MATCHES(",string,",",pattern,"||'$')")
                   },
                   str_pad = function(string, width, side="left",pad=" ",use_length=FALSE){
                     if (side %in% c("left")) {
                       sql_expr(LPAD(!!string,!!as.integer(width),!!pad))
                     }
                     else if (side %in% c("right")) {
                       sql_expr(RPAD(!!string,!!as.integer(width),!!pad))
                     }
                     else if (side %in% c("both")) {
                       sql_expr(RPAD(REPEAT(!!pad,(!!as.integer(width)-LENGTH(!!string))/2L) %||% !!string,!!as.integer(width),!!pad))
                     }
                     else {
                       stop('Argument \'side\' should be "left", "right" or "both"', call. = FALSE)
                     }
                   }
    ),
    sql_translator(.parent = base_agg,
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
    sql_translator(.parent = base_win,
                   cor = win_aggregate_2("CORR"),
                   cov = win_aggregate_2("COVAR_SAMP"),
                   sd =  win_aggregate("STDDEV"),
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

#' @export
sql_expr_matches.duckdb_connection <- function(con, x, y) {
  build_sql <- pkg_method("build_sql","dbplyr")
  # https://duckdb.org/docs/sql/expressions/comparison_operators
  build_sql(x, " IS NOT DISTINCT FROM ", y, con = con)
}

#' @export
dbplyr_fill0.duckdb_connection <- function (.con, .data, cols_to_fill, order_by_cols, .direction) {
  dbplyr_fill0 <- pkg_method("dbplyr_fill0.SQLiteConnection","dbplyr")

  # Required because of the bug in dbplyr (con is not passed to "translate_sql(cumsum..." call)
  setcon <- pkg_method("set_current_con","dbplyr")
  setcon(.con)

  dbplyr_fill0(.con, .data, cols_to_fill, order_by_cols, .direction)
}

# Needed to suppress the R CHECK notes (due to the use of sql_expr)
globalVariables(c("REGEXP_MATCHES","CAST","%AS%","INTEGER","XOR","%<<%","%>>%","LN","LOG","ROUND","EXTRACT","%FROM%","MONTH","STRFTIME","QUARTER","YEAR","DATE_TRUNC","DATE","DOY","TO_SECONDS","BIGINT","TO_MINUTES","TO_HOURS","TO_DAYS","TO_WEEKS","TO_MONTHS","TO_YEARS","STRPOS","NOT","REGEXP_REPLACE","TRIM","LPAD","RPAD","%||%","REPEAT","LENGTH","STRING_AGG"))


