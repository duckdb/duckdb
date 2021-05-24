repo <- "https://cloud.r-project.org/"
install.packages(c("DBI", "callr", "DBItest", "dbplyr", "nycflights13", "testthat"), repos=repo)
install.packages("testthat", type="source", repos=repo)