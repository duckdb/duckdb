library("DBI")
library("dplyr")

dsrc <- duckdb::src_duckdb()

# taken from the dbplyr vignette
# https://cran.r-project.org/web/packages/dbplyr/vignettes/dbplyr.html

copy_to(dsrc, nycflights13::flights, "flights", temporary = FALSE)

flights_db <- tbl(dsrc, "flights")

flights_db %>% 
  group_by(dest) %>%
  summarise(delay = mean(dep_time))

