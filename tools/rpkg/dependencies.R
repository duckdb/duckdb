local({
  pkg <- c("DBI", "callr", "DBItest", "dbplyr", "nycflights13", "testthat", "bit64", "cpp11", "arrow", "covr")

  if (.Platform$OS.type == "unix") {
    options(HTTPUserAgent = sprintf("R/4.1.0 R (4.1.0 %s)", paste(R.version$platform, R.version$arch, R.version$os)))
    install.packages(pkg, repos = "https://packagemanager.rstudio.com/all/__linux__/focal/latest")
  } else {
    install.packages(pkg, repos = "https://cloud.r-project.org", pkgType = "binary")
  }
})
