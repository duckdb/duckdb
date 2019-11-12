#' @details TBD.
"_PACKAGE"

check_flag <- function(x) {
  if (is.null(x)   || is.na(x) || !is.logical(x) || length(x) != 1) {
    stop("flags need to be scalar logicals")
  }
}

#' @useDynLib duckdb , .registration = TRUE
