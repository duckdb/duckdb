library(DBI)
library(duckdb)

# create / connect to database file
con <- dbConnect(duckdb(dbdir = "${1:database=quack.db}"))

# write a table to it
dbWriteTable(con, "iris", iris)

# and disconnect
dbDisconnect(con, shutdown=TRUE)
