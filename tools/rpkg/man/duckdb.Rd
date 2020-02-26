\name{duckdb}
\alias{duckdb}
\title{DuckDB DBI Driver}
\usage{
duckdb(dbdir = ":memory:", read_only = FALSE)
}
\description{
TBD.
}
\examples{
library(DBI)

con1 <- dbConnect(duckdb::duckdb())
dbDisconnect(con1, shutdown=TRUE)

con2 <- dbConnect(duckdb::duckdb())
duckdb::duckdb_shutdown()

con3 <- dbConnect(duckdb::duckdb(dbdir=":memory"))
dbDisconnect(con3, shutdown=TRUE)

con4 <- dbConnect(duckdb::duckdb(), dbdir=":memory")
dbDisconnect(con4, shutdown=TRUE)
}
