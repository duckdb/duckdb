options(HTTPUserAgent = sprintf("R/%s R (%s)", getRversion(), paste(getRversion(), R.version$platform, R.version$arch, R.version$os))
install.packages(
  c("DBI", "callr", "DBItest", "dbplyr", "nycflights13", "testthat", "bit64", "cpp11", "arrow", "covr"),
  repos = "https://packagemanager.rstudio.com/all/__linux__/focal/latest"
)
