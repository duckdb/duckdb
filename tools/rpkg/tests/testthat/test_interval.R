expect_equal_difftime <- function (a, b)
    expect_equal(as.numeric(a, units = "secs"), as.numeric(b, units = "secs"))

test_that("we can retrieve an interval", {
    con <- dbConnect(duckdb())
    on.exit(dbDisconnect(con, shutdown = TRUE))

    res <- dbGetQuery(con, "SELECT '2021-11-26'::TIMESTAMP-'1984-04-24'::TIMESTAMP i")
    expect_equal_difftime(as.Date('2021-11-26') - as.Date('1984-04-24'), res$i)

    res <- dbGetQuery(con, "SELECT '2021-11-26 12:01:00'::TIMESTAMP-'2021-11-26 12:00:00'::TIMESTAMP i")
    expect_equal_difftime(as.POSIXct('2021-11-26 12:01:00') - as.POSIXct('2021-11-26 12:00:00'), res$i)

    res <- dbGetQuery(con, "SELECT '1984-04-24'::TIMESTAMP-'2021-11-26'::TIMESTAMP i")
    expect_equal_difftime(as.Date('1984-04-24') - as.Date('2021-11-26'), res$i)

    res <- dbGetQuery(con, "SELECT '2021-11-26 12:00:00'::TIMESTAMP - '2021-11-26 12:01:00'::TIMESTAMP i")
    expect_equal_difftime(as.POSIXct('2021-11-26 12:00:00') - as.POSIXct('2021-11-26 12:01:00'), res$i)

    res <- dbGetQuery(con, "SELECT NULL::INTERVAL i")
    expect_true(is.na(res$i))
})
