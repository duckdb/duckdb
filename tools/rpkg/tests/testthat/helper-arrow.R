# if not running on cran, we run arrow tests.
# we import arrow first since there are GC issues if we import it after
# running relational R tests.
# something to do with how arrow releases its resources.
if (Sys.getenv("NOT_CRAN") == "true") {
    requireNamespace("arrow", quietly = TRUE)
}
