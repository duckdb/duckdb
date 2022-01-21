
local({

  pkgs <- c("gert", "remotes", "callr", "rlang", "bench", "tibble", "ggplot2",
            "tidyr")
  avail <- pkgs %in% installed.packages()

  if (!all(avail)) {
    stop("Package(s) ", paste(pkgs[!avail], collapse = ", "), " are required. ",
         "Please install.")
  }
})

install_gh <- function(lib, repo = "duckdb/duckdb", branch = NULL,
                       ref = NULL, subdir = "tools/rpkg", update_deps = FALSE) {

  dir <- gert::git_clone(paste0("https://github.com/", repo), tempfile(),
                         branch = branch)

  on.exit(unlink(dir, recursive = TRUE))

  if (!is.null(ref)) {
    branch <- paste0("bench_", rand_string(alphabet = c(letters, 0:9)))
    gert::git_branch_create(branch, ref = ref, checkout = TRUE, repo = dir)
    on.exit(gert::git_branch_delete(branch, repo = dir), add = TRUE,
            after = FALSE)
  }

  pkg <- file.path(dir, subdir)
  arg <- c(pkg, paste0("--", c(paste0("library=", lib), "no-multiarch")))

  if (isTRUE(update_deps)) {
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

write_df <- function(con, dat, tbl = rand_string()) {
  dbWriteTable(con, tbl, dat)
  dbRemoveTable(con, tbl)
}

register_df <- function(con, dat, tbl = rand_string()) {
  duckdb_register(con, tbl, dat)
  duckdb_unregister(con, tbl)
}

register_arrow <- function(con, dat, tbl = rand_string()) {
  duckdb_register_arrow(con, tbl, dat)
  duckdb_unregister_arrow(con, tbl)
}

select_some <- function(con, tbl) {
  dbGetQuery(con,
    paste("SELECT * FROM", dbQuoteIdentifier(con, tbl), "WHERE",
          dbQuoteIdentifier(con, "v3"), "> 50")
  )
}

bench_mark <- function(versions, ..., grid = NULL, setup = NULL,
                       teardown = NULL, seed = NULL, helpers = NULL,
                       pkgs = NULL, reps = 1L) {

  eval_versions <- function(lib, args) {

    eval_grid <- function(args, ...) {

      eval_one <- function(args, setup, teardown, exprs, nreps, seed, helpers,
                           libs) {

        if (!is.null(seed)) set.seed(seed)

        for (helper in helpers) source(helper)
        for (lib in libs) library(lib, character.only = TRUE)

        env <- rlang::new_data_mask(new.env(parent = emptyenv()))

        for (nme in names(args)) {
          assign(nme, args[[nme]], envir = env)
        }

        message(paste0(names(args), ": ", args, collapse = ", "))

        rlang::eval_tidy(setup, data = env)
        on.exit(rlang::eval_tidy(teardown, data = env))

        res <- bench::mark(iterations = nreps, check = FALSE, exprs = exprs,
                           env = env, time_unit = "s")

        arg <- lapply(args, rep, nrow(res))

        do.call(tibble::add_column, c(list(res), arg, list(.before = 2)))
      }

      do.call(rbind, lapply(args, eval_one, ...))
    }

    callr::r(eval_grid, args, libpath = lib, show = TRUE)
  }

  stopifnot(all(pkgs %in% installed.packages()))

  exprs <- rlang::exprs(...)
  setup <- rlang::enquo(setup)
  teardown <- rlang::enquo(teardown)

  params <- expand.grid(grid, stringsAsFactors = FALSE)
  params <- split(params, seq(nrow(params)))

  res <- lapply(
    vapply(versions, `[[`, character(1L), "lib"),
    eval_versions,
    list(params, setup, teardown, exprs, reps, seed, helpers, pkgs)
  )

  res <- Map(
    tibble::add_column,
    res,
    version = Map(rep, names(res), vapply(res, nrow, integer(1L))),
    MoreArgs = list(.before = 2)
  )

  do.call(rbind, res)
}

bench_plot <- function (object, type = c("beeswarm", "jitter", "ridge",
                                         "boxplot", "violin"), ...) {

  type <- match.arg(type)

  if (type == "beeswarm" && !requireNamespace("ggbeeswarm", quietly = TRUE)) {
    stop("`ggbeeswarm` must be installed to use `type = \"beeswarm\"` option.")
  }

  if (inherits(object$expression, "bench_expr")) {
    object$expression <- names(object$expression)
  }

  if (utils::packageVersion("tidyr") > "0.8.99") {
    res <- tidyr::unnest(object, c(time, gc))
  } else {
    res <- tidyr::unnest(object)
  }

  p <- ggplot2::ggplot(res)

  p <- switch(
    type,
    beeswarm = p + ggplot2::aes_string("version", "time", color = "gc") +
      ggbeeswarm::geom_quasirandom(...) +
      ggplot2::coord_flip(),
    jitter = p + ggplot2::aes_string("version", "time", color = "gc") +
      ggplot2::geom_jitter(...) +
      ggplot2::coord_flip(),
    ridge = p + ggplot2::aes_string("time", "version") +
      ggridges::geom_density_ridges(...),
    boxplot = p + ggplot2::aes_string("version", "time") +
      ggplot2::geom_boxplot(...) +
      ggplot2::coord_flip(),
    violin = p + ggplot2::aes_string("version", "time") +
      ggplot2::geom_violin(...) +
      ggplot2::coord_flip()
  )

  parameters <- setdiff(colnames(object), c("version", bench:::summary_cols,
                        bench:::data_cols, c("level0", "level1", "level2"),
                        "expression"))

  labeller <- function(...) {
    sub_fun <- function(x) sub("^expression: ", "", x)
    lapply(ggplot2::label_both(...), sub_fun)
  }

  if (length(parameters) > 0) {
    p <- p + ggplot2::facet_grid(
      paste("expression", "~", paste(parameters, collapse = "+")),
      labeller = labeller, scales = "free_x"
    )
  }

  p + ggplot2::labs(y = "Time [s]", color = "GC level") +
    ggplot2::theme(axis.title.y = ggplot2::element_blank(),
                   legend.position = "bottom")
}
