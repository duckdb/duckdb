# snapshots of dbplyr generic scalar translation

    Code
      translate(as.character(1))
    Output
      <SQL> CAST(1.0 AS TEXT)
    Code
      translate(as.character(1L))
    Output
      <SQL> CAST(1 AS TEXT)
    Code
      translate(as.numeric(1))
    Output
      <SQL> CAST(1.0 AS NUMERIC)
    Code
      translate(as.double(1.2))
    Output
      <SQL> CAST(1.2 AS NUMERIC)
    Code
      translate(as.integer(1.2))
    Output
      <SQL> CAST(1.2 AS INTEGER)
    Code
      translate(as.integer64(1.2))
    Output
      <SQL> CAST(1.2 AS BIGINT)
    Code
      translate(as.logical("TRUE"))
    Output
      <SQL> CAST('TRUE' AS BOOLEAN)
    Code
      translate(tolower("HELLO"))
    Output
      <SQL> LOWER('HELLO')
    Code
      translate(toupper("hello"))
    Output
      <SQL> UPPER('hello')
    Code
      translate(pmax(1, 2, na.rm = TRUE))
    Output
      <SQL> GREATEST(1.0, 2.0)
    Code
      translate(pmin(1, 2, na.rm = TRUE))
    Output
      <SQL> LEAST(1.0, 2.0)
    Code
      translate(as.character("2020-01-01"))
    Output
      <SQL> CAST('2020-01-01' AS TEXT)
    Code
      translate(c("2020-01-01", "2020-13-02"))
    Output
      <SQL> ('2020-01-01', '2020-13-02')
    Code
      translate(iris[["sepal_length"]])
    Output
      <SQL> iris.sepal_length
    Code
      translate(iris[[1]])
    Output
      <SQL> iris[1]
    Code
      translate(cot(x))
    Output
      <SQL> COT(x)
    Code
      translate(substr("test", 2, 3))
    Output
      <SQL> SUBSTR('test', 2, 2)

# snapshots of duckdb custom scalars translations

    Code
      translate(as.raw(10))
    Output
      <SQL> CAST(10.0 AS VARBINARY)
    Code
      translate(13 %% 5)
    Output
      <SQL> FMOD(13.0, 5.0)
    Code
      translate(35.8 %/% 4)
    Output
      <SQL> FDIV(35.8, 4.0)
    Code
      translate(35.8^2.51)
    Output
      <SQL> POW(35.8, 2.51)
    Code
      translate(bitwOr(x, 128L))
    Output
      <SQL> (CAST(x AS INTEGER)) | (CAST(128 AS INTEGER))
    Code
      translate(bitwAnd(x, 128))
    Output
      <SQL> (CAST(x AS INTEGER)) & (CAST(128.0 AS INTEGER))
    Code
      translate(bitwXor(x, 128L))
    Output
      <SQL> XOR((CAST(x AS INTEGER)), (CAST(128 AS INTEGER)))
    Code
      translate(bitwNot(x))
    Output
      <SQL> ~(CAST(x AS INTEGER))
    Code
      translate(bitwShiftL(x, 5L))
    Output
      <SQL> (CAST(x AS INTEGER)) << (CAST(5 AS INTEGER))
    Code
      translate(bitwShiftR(x, 4L))
    Output
      <SQL> (CAST(x AS INTEGER)) >> (CAST(4 AS INTEGER))
    Code
      translate(log(x))
    Output
      <SQL> LN(x)
    Code
      translate(log(x, base = 5))
    Output
      <SQL> LOG(x) / LOG(5.0)
    Code
      translate(log(x, base = 10))
    Output
      <SQL> LOG10(x)
    Code
      translate(log(x, base = 2))
    Output
      <SQL> LOG2(x)
    Code
      translate(log10(x))
    Output
      <SQL> LOG10(x)
    Code
      translate(log2(x))
    Output
      <SQL> LOG2(x)
    Code
      translate(is.nan(var1))
    Output
      <SQL> (var1 IS NOT NULL AND PRINTF('%f', var1) = 'nan')
    Code
      translate(is.infinite(var1))
    Output
      <SQL> (var1 IS NOT NULL AND REGEXP_MATCHES(PRINTF('%f', var1), 'inf'))
    Code
      translate(is.finite(var1))
    Output
      <SQL> (NOT (var1 IS NULL OR REGEXP_MATCHES(PRINTF('%f', var1), 'inf|nan')))
    Code
      translate(grepl("pattern", text))
    Output
      <SQL> REGEXP_MATCHES("text", 'pattern')
    Code
      translate(grepl("pattern", text, ignore.case = TRUE))
    Output
      <SQL> REGEXP_MATCHES("text", '(?i)pattern')
    Code
      translate(regexpr("pattern", text))
    Output
      <SQL> (CASE WHEN REGEXP_MATCHES("text", 'pattern') THEN (LENGTH(LIST_EXTRACT(STRING_SPLIT_REGEX("text", 'pattern'), 0))+1) ELSE -1 END)
    Code
      translate(round(x, digits = 1.1))
    Output
      <SQL> ROUND(x, CAST(ROUND(1.1, 0) AS INTEGER))
    Code
      translate(as.Date("2019-01-01"))
    Output
      <SQL> CAST('2019-01-01' AS DATE)
    Code
      translate(as.POSIXct("2019-01-01 01:01:01"))
    Output
      <SQL> CAST('2019-01-01 01:01:01' AS TIMESTAMP)

# snapshot tests for pasting translate

    Code
      translate(paste("hi", "bye"))
    Output
      <SQL> CONCAT_WS(' ', 'hi', 'bye')
    Code
      translate(paste("hi", "bye", sep = "-"))
    Output
      <SQL> CONCAT_WS('-', 'hi', 'bye')
    Code
      translate(paste0("hi", "bye"))
    Output
      <SQL> CONCAT_WS('', 'hi', 'bye')
    Code
      translate(paste(x, y), window = FALSE)
    Output
      <SQL> CONCAT_WS(' ', x, y)
    Code
      translate(paste0(x, y), window = FALSE)
    Output
      <SQL> CONCAT_WS('', x, y)

# snapshots for custom lubridate functions translated correctly

    Code
      translate(yday(x))
    Output
      <SQL> EXTRACT(DOY FROM x)
    Code
      translate(quarter(x))
    Output
      <SQL> EXTRACT(QUARTER FROM x)
    Code
      translate(quarter(x))
    Output
      <SQL> EXTRACT(QUARTER FROM x)
    Code
      translate(quarter(x, type = "year.quarter"))
    Output
      <SQL> (EXTRACT(YEAR FROM x) || '.' || EXTRACT(QUARTER FROM x))
    Code
      translate(quarter(x, type = "quarter"))
    Output
      <SQL> EXTRACT(QUARTER FROM x)
    Code
      translate(quarter(x, type = TRUE))
    Output
      <SQL> (EXTRACT(YEAR FROM x) || '.' || EXTRACT(QUARTER FROM x))
    Code
      translate(quarter(x, type = FALSE))
    Output
      <SQL> EXTRACT(QUARTER FROM x)
    Code
      translate(quarter(x, type = "date_first"))
    Output
      <SQL> (CAST(DATE_TRUNC('QUARTER', x) AS DATE))
    Code
      translate(quarter(x, type = "date_last"))
    Output
      <SQL> (CAST((DATE_TRUNC('QUARTER', x) + INTERVAL '1 QUARTER' - INTERVAL '1 DAY') AS DATE))
    Code
      translate(month(x, label = FALSE))
    Output
      <SQL> EXTRACT(MONTH FROM x)
    Code
      translate(month(x, label = TRUE))
    Output
      <SQL> STRFTIME(x, '%b')
    Code
      translate(month(x, label = TRUE, abbr = FALSE))
    Output
      <SQL> STRFTIME(x, '%B')
    Code
      translate(qday(x))
    Output
      <SQL> DATE_DIFF('DAYS', DATE_TRUNC('QUARTER', CAST((x) AS DATE)), (CAST((x) AS DATE) + INTERVAL '1 DAY'))
    Code
      translate(wday(x))
    Output
      <SQL> EXTRACT('dow' FROM CAST(x AS DATE) + 0) + 1
    Code
      translate(wday(x, week_start = 4))
    Output
      <SQL> EXTRACT('dow' FROM CAST(x AS DATE) + 3) + 1
    Code
      translate(wday(x, label = TRUE))
    Output
      <SQL> STRFTIME(x, '%a')
    Code
      translate(wday(x, label = TRUE, abbr = FALSE))
    Output
      <SQL> STRFTIME(x, '%A')
    Code
      translate(seconds(x))
    Output
      <SQL> TO_SECONDS(CAST(x AS BIGINT))
    Code
      translate(minutes(x))
    Output
      <SQL> TO_MINUTES(CAST(x AS BIGINT))
    Code
      translate(hours(x))
    Output
      <SQL> TO_HOURS(CAST(x AS BIGINT))
    Code
      translate(days(x))
    Output
      <SQL> TO_DAYS(CAST(x AS INTEGER))
    Code
      translate(weeks(x))
    Output
      <SQL> TO_DAYS(7 * CAST(x AS INTEGER))
    Code
      translate(months(x))
    Output
      <SQL> TO_MONTHS(CAST(x AS INTEGER))
    Code
      translate(years(x))
    Output
      <SQL> TO_YEARS(CAST(x AS INTEGER))
    Code
      translate(floor_date(x, "month"))
    Output
      <SQL> DATE_TRUNC('month', x)
    Code
      translate(floor_date(x, "week"))
    Output
      <SQL> CAST(x AS DATE) - CAST(EXTRACT('dow' FROM CAST(x AS DATE) + 0) AS INTEGER)
    Code
      translate(floor_date(x, "week", week_start = 1))
    Output
      <SQL> DATE_TRUNC('week', x)
    Code
      translate(floor_date(x, "week", week_start = 4))
    Output
      <SQL> CAST(x AS DATE) - CAST(EXTRACT('dow' FROM CAST(x AS DATE) + 3) AS INTEGER)

# snapshots for custom stringr functions translated correctly

    Code
      translate(str_c(x, y))
    Output
      <SQL> CONCAT_WS('', x, y)
    Code
      translate(str_detect(x, y))
    Output
      <SQL> REGEXP_MATCHES(x, y)
    Code
      translate(str_detect(x, y, negate = TRUE))
    Output
      <SQL> (NOT(REGEXP_MATCHES(x, y)))
    Code
      translate(str_replace(x, y, z))
    Output
      <SQL> REGEXP_REPLACE(x, y, z)
    Code
      translate(str_replace_all(x, y, z))
    Output
      <SQL> REGEXP_REPLACE(x, y, z, 'g')
    Code
      translate(str_squish(x))
    Output
      <SQL> TRIM(REGEXP_REPLACE(x, '\s+', ' ', 'g'))
    Code
      translate(str_remove(x, y))
    Output
      <SQL> REGEXP_REPLACE(x, y, '')
    Code
      translate(str_remove_all(x, y))
    Output
      <SQL> REGEXP_REPLACE(x, y, '', 'g')
    Code
      translate(str_to_sentence(x))
    Output
      <SQL> (UPPER(x[0]) || x[1:NULL])
    Code
      translate(str_starts(x, y))
    Output
      <SQL> REGEXP_MATCHES(x,'^(?:'||y))
    Code
      translate(str_ends(x, y))
    Output
      <SQL> REGEXP_MATCHES((?:x,y||')$')
    Code
      translate(str_pad(x, width = 10))
    Output
      <SQL> LPAD(x, CAST(GREATEST(10, LENGTH(x)) AS INTEGER), ' ')
    Code
      translate(str_pad(x, width = 10, side = "right"))
    Output
      <SQL> RPAD(x, CAST(GREATEST(10, LENGTH(x)) AS INTEGER), ' ')
    Code
      translate(str_pad(x, width = 10, side = "both", pad = "<"))
    Output
      <SQL> RPAD(REPEAT('<', (10 - LENGTH(x)) / 2) || x, CAST(GREATEST(10, LENGTH(x)) AS INTEGER), '<')

# snapshots datetime escaping working as in DBI

    Code
      test_date <- as.Date("2020-01-01")
      escape(test_date, con = con)
    Output
      <SQL> '2020-01-01'::date
    Code
      escape("2020-01-01", con = con)
    Output
      <SQL> '2020-01-01'
    Code
      test_datetime <- as.POSIXct("2020-01-01 01:23:45 UTC", tz = "UTC")
      escape(test_datetime, con = con)
    Output
      <SQL> '2020-01-01 01:23:45'::timestamp
    Code
      escape("2020-01-01 01:23:45 UTC", con = con)
    Output
      <SQL> '2020-01-01 01:23:45 UTC'
    Code
      test_datetime_tz <- as.POSIXct("2020-01-01 18:23:45 UTC", tz = "America/Los_Angeles")
      escape(test_datetime_tz, con = con)
    Output
      <SQL> '2020-01-02 02:23:45'::timestamp
    Code
      escape("2020-01-01 18:23:45 PST", con = con)
    Output
      <SQL> '2020-01-01 18:23:45 PST'

# two variable aggregates are translated correctly

    Code
      translate(cor(x, y), window = FALSE)
    Output
      <SQL> CORR(x, y)
    Code
      translate(cor(x, y), window = TRUE)
    Output
      <SQL> CORR(x, y) OVER ()

# these should give errors

    Code
      translate(grepl("dummy", txt, perl = TRUE))
    Error <simpleError>
      Parameters `perl`, `fixed` and `useBytes` in grepl are not currently supported in DuckDB backend
    Code
      translate(quarter(x, type = "other"))
    Error <simpleError>
      Unsupported type other
    Code
      translate(quarter(x, fiscal_start = 2))
    Error <simpleError>
      `fiscal_start` is not yet supported in DuckDB translation. Must be 1.
    Code
      translate(str_pad(x, width = 10, side = "other"))
    Error <simpleError>
      Argument 'side' should be "left", "right" or "both"

