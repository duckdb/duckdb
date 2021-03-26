library(DBI)
library(duckdb)

# create / connect to database file
drv <- duckdb(dbdir = "${1:database=quack.db}")
con <- dbConnect(drv)

## write a table to it
# dbWriteTable(con, "iris", iris)

## and disconnect
# dbDisconnect(con, shutdown=TRUE)
