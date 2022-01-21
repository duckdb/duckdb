
local({

  pkgs <- c("gert", "remotes", "callr", "rlang", "bench", "tibble")
  avail <- pkgs %in% installed.packages()

  if (!all(avail)) {
    stop("Package(s) ", paste(pkgs[!avail], collapse = ", "), " are required. ",
         "Please install.")
  }
})

install_gh <- function(lib, repo = "duckdb/duckdb", branch = NULL,
                       ref = NULL, subdir = "tools/rpkg", update = TRUE) {

  dir <- gert::git_clone(paste0("https://github.com/", repo), tempfile(),
                         branch = branch)

  on.exit(unlink(dir, recursive = TRUE))

  if (!is.null(ref)) {
    branch <- paste0("bench_", rand_string(alphabet = c(letters, 0:9)))
    gert::git_branch_create(branch, ref = ref, checkout = TRUE, repo = dir)
    on.exit(gert::git_branch_delete(branch, repo = dir), add = TRUE)
  }

  pkg <- file.path(dir, subdir)
  arg <- c(pkg, paste0("--", c(paste0("library=", lib), "no-multiarch")))

  if (isTRUE(update)) {
    remotes::install_deps(pkg, upgrade = "always")
  }

  res <- callr::rcmd_safe("INSTALL", arg, show = TRUE, fail_on_status = TRUE)

  invisible(NULL)
}

install_one <- function(what, ...) do.call(install_gh, c(what, list(...)))

install_all <- function(lst, ...) invisible(lapply(lst, install_one, ...))

dir_create <- function(paths = tempfile(), ...) {

  paths <- file.path(paths, ...)

  for (path in paths) {
    if (!dir.exists(path)) {
      dir.create(path)
    }
  }

  paths
}

rand_string <- function(length = 8, alphabet = c(letters, LETTERS, 0:9)) {
  paste0(sample(alphabet, length, replace = TRUE), collapse = "")
}

setup_data <- function(nrow = 1e3) {

  rand_strings <- function(n, ...) {
    vapply(integer(n), function(i, ...) rand_string(...), character(1L), ...)
  }

  rand_fact <- function(n, levels = rand_strings(5)) {
    structure(
      sample(length(levels), n, TRUE), levels = levels, class = "factor"
    )
  }

  data.frame(
    v1 = sample.int(5, nrow, TRUE),
    v2 = sample.int(nrow, nrow),
    v3 = runif(nrow, max = 100),
    v4 = rand_strings(nrow),
    v5 = rand_fact(nrow)
  )
}

write_table <- function(con, dat, tbl = rand_string()) {
  dbWriteTable(con, tbl, dat)
  dbRemoveTable(con, tbl)
}

bench_mark <- function(versions, ..., grid = NULL, setup = NULL,
                        teardown = NULL, seed = NULL, helpers = NULL,
                        pkgs = NULL, iterations = 1L) {

  eval_versions <- function(lib, args) {

    eval_proc <- function(args, ...) {

      eval_one <- function(args, setup, teardown, exprs, niter, seed, helpers,
                           libs) {

        if (!is.null(seed)) set.seed(seed)

        for (helper in helpers) source(helper)
        for (lib in libs) library(lib, character.only = TRUE)

        env <- rlang::new_data_mask(new.env(parent = emptyenv()))

        for (nme in names(args)) {
          assign(nme, args[[nme]], envir = env)
        }

        rlang::eval_tidy(setup, data = env)
        on.exit(rlang::eval_tidy(teardown, data = env))

        res <- bench::mark(iterations = niter, check = FALSE, exprs = exprs,
                           env = env)

        arg <- lapply(args, rep, nrow(res))

        do.call(tibble::add_column, c(list(res), arg, list(.before = 2)))
      }

      do.call(rbind, lapply(args, eval_one, ...))
    }

    callr::r(eval_proc, args, libpath = lib, show = TRUE)
  }

  exprs <- rlang::exprs(...)
  setup <- rlang::enquo(setup)
  teardown <- rlang::enquo(teardown)

  params <- expand.grid(grid, stringsAsFactors = FALSE)
  params <- split(params, seq(nrow(params)))

  res <- lapply(
    vapply(versions, `[[`, character(1L), "lib"),
    eval_versions,
    list(params, setup, teardown, exprs, iterations, seed, helpers, pkgs)
  )

  res <- Map(
    tibble::add_column,
    res,
    version = Map(rep, names(res), vapply(res, nrow, integer(1L))),
    MoreArgs = list(.before = 2)
  )

  do.call(rbind, res)
}
