install_args <- list(
	pkgs=c("DBI", "DBItest", "testthat", "dbplyr", "RSQLite", "callr", "nycflights13"),
	repos=c("http://cran.rstudio.com/"))

if (.Platform$OS.type == "windows") {
	message("Forcing binary package installs on Windows :/")
	install_args[['type']] <- "binary"
}

do.call(install.packages, install_args)
