options(HTTPUserAgent = sprintf("R/4.1.0 R (4.1.0 %s)", paste(R.version$platform, R.version$arch, R.version$os)))
install.packages(
  c("DBI", "callr", "DBItest", "dbplyr", "nycflights13", "testthat", "bit64", "cpp11", "arrow", "covr"),
  repos = "https://packagemanager.rstudio.com/all/__linux__/focal/latest"
)
