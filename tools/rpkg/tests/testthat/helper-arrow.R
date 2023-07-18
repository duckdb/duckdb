
not_cran = Sys.getenv("NOT_CRAN")
if (not_cran == "true") {
    requireNamespace("arrow", quietly = TRUE)
}
