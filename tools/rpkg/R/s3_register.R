# nocov start

# From: https://github.com/DyfanJones/noctua/blob/b82113098df6b3a7981cf8ca0c1ae9f2ff408756/R/utils.R#L168-L175
# get parent pkg function and method
pkg_method <- function(fun, pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(fun, " requires the ", pkg, " package, please install it first and try again",
      call. = FALSE
    )
  }
  fun_name <- utils::getFromNamespace(fun, pkg)
  return(fun_name)
}

# nocov end
