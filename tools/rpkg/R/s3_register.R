# nocov start

# From: https://github.com/r-lib/rlang/blob/d5df93251d055721abb4a576433fb867ca40d527/R/compat-s3-register.R#L53-L122
s3_register <- function(generic, class, method = NULL) {
  stopifnot(is.character(generic), length(generic) == 1)
  stopifnot(is.character(class), length(class) == 1)

  pieces <- strsplit(generic, "::")[[1]]
  stopifnot(length(pieces) == 2)
  package <- pieces[[1]]
  generic <- pieces[[2]]

  caller <- parent.frame()

  get_method_env <- function() {
    top <- topenv(caller)
    if (isNamespace(top)) {
      asNamespace(environmentName(top))
    } else {
      caller
    }
  }
  get_method <- function(method) {
    if (is.null(method)) {
      get(paste0(generic, ".", class), envir = get_method_env())
    } else {
      method
    }
  }

  register <- function(...) {
    envir <- asNamespace(package)

    # Refresh the method each time, it might have been updated by
    # `devtools::load_all()`
    method_fn <- get_method(method)
    stopifnot(is.function(method_fn))


    # Only register if generic can be accessed
    if (exists(generic, envir)) {
      registerS3method(generic, class, method_fn, envir = envir)
    } else if (identical(Sys.getenv("NOT_CRAN"), "true")) {
      warning(sprintf(
        "Can't find generic `%s` in package %s to register S3 method.",
        generic,
        package
      ))
    }
  }

  # Always register hook in case package is later unloaded & reloaded
  setHook(packageEvent(package, "onLoad"), function(...) {
    register()
  })

  # Avoid registration failures during loading (pkgload or regular)
  if (isNamespaceLoaded(package) && environmentIsLocked(asNamespace(package))) {
    register()
  }

  invisible()
}

# From: https://github.com/DyfanJones/noctua/blob/b82113098df6b3a7981cf8ca0c1ae9f2ff408756/R/utils.R#L168-L175
# get parent pkg function and method
pkg_method <- function(fun, pkg, caller_fun = fun, version = NULL) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(caller_fun, " requires the ", pkg, " package, please install it first and try again",
      call. = FALSE
    )
  }

  if (!is.null(version)) {
    spec <- get(".__NAMESPACE__.", asNamespace(pkg))$spec
    pkg_version <- base::package_version(spec[["version"]])
    if (pkg_version < version) {
      stop(caller_fun, " requires the ", pkg, " package in version ", version,
        ", you have version ", pkg_version, ". Please update it first and try again",
        call. = FALSE
      )
    }
  }

  fun_name <- utils::getFromNamespace(fun, pkg)
  return(fun_name)
}

# nocov end
