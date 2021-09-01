install.packages(c("DBI", "callr", "DBItest", "dbplyr", "nycflights13", "testthat", "bit64", "cpp11"), repos="https://cloud.r-project.org/", type="source", Ncpus=parallel::detectCores())
install.packages("arrow", repos = c("https://arrow-r-nightly.s3.amazonaws.com", "https://cloud.r-project.org/"))
