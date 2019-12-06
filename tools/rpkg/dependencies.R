type <- "both"
if (.Platform$OS.type == "windows") {
	message("Forcing binary package installs on Windows :/")
	type <- "binary"
}
install.packages(c("DBI", "DBItest", "testthat", "dbplyr", "RSQLite", "callr", "nycflights13"), repos=c("http://cran.rstudio.com/"), type=type)
