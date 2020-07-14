# installs dependencies, runs R CMD check, runs covr::codecov()
# if (Sys.info()["sysname"] == "Windows") {
#   do_package_checks(args = c("--no-manual", "--no-multiarch", "--no-byte-compile"))
# }
do_package_checks()
