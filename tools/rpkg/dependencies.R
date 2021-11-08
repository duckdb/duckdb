install.packages(c("DBI", "callr", "DBItest", "dbplyr", "nycflights13", "testthat", "bit64", "cpp11"), repos="https://cloud.r-project.org/", type="source", Ncpus=parallel::detectCores())
packageurl <- "https://cran.r-project.org/src/contrib/Archive/arrow/arrow_5.0.0.tar.gz"
install.packages(packageurl, repos=NULL, type="source")
