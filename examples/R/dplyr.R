library("DBI")
library("dplyr")

library("DBI")

# create a DuckDB connection, either as a temporary in-memory database (default) or with a file
con <- dbConnect(duckdb::duckdb(), ":memory:")

# taken from the dbplyr vignette
# https://cran.r-project.org/web/packages/dbplyr/vignettes/dbplyr.html

copy_to(con, nycflights13::flights, "flights", temporary = FALSE)

flights_db <- tbl(con, "flights")

flights_db %>%
  group_by(dest) %>%
  summarise(delay = mean(dep_time, na.rm = TRUE))
