
local({
  pkgs <- c(
    "gert", "remotes", "callr", "rlang", "bench", "ggplot2",
    "tidyr"
  )
  avail <- pkgs %in% installed.packages()

  if (!all(avail)) {
    stop(
      "Package(s) ", paste(pkgs[!avail], collapse = ", "), " are required. ",
      "Please install."
    )
  }
})

install_gh <- function(lib, repo = "duckdb/duckdb", branch = NULL,
                       ref = NULL, subdir = "tools/rpkg", update_deps = FALSE) {
  dir <- gert::git_clone(paste0("https://github.com/", repo), tempfile(),
    branch = branch
  )

  on.exit(unlink(dir, recursive = TRUE))

  if (!is.null(ref)) {
    current <- gert::git_branch(repo = dir)
    branch <- paste0("bench_", rand_string(alphabet = c(letters, 0:9)))
    gert::git_branch_create(branch, ref = ref, checkout = TRUE, repo = dir)
    on.exit(
      {
        gert::git_branch_checkout(current, repo = dir)
        gert::git_branch_delete(branch, repo = dir)
      },
      add = TRUE,
      after = FALSE
    )
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
      sample(length(levels), n, TRUE),
      levels = levels, class = "factor"
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
  dbGetQuery(
    con,
    paste(
      "SELECT * FROM", dbQuoteIdentifier(con, tbl), "WHERE",
      dbQuoteIdentifier(con, "v3"), "> 50"
    )
  )
}

bench_mark <- function(versions, ..., grid = NULL, setup = NULL,
                       teardown = NULL, seed = NULL, helpers = NULL,
                       pkgs = NULL, reps = 1L) {
  eval_versions <- function(lib, vers, args) {
    eval_grid <- function(args, ...) {
      eval_one <- function(args, setup, teardown, exprs, nreps, seed, helpers,
                           libs, vers) {
        if (!is.null(seed)) set.seed(seed)

        for (helper in helpers) source(helper)
        for (lib in libs) library(lib, character.only = TRUE)

        env <- rlang::new_data_mask(new.env(parent = emptyenv()))

        for (nme in names(args)) {
          assign(nme, args[[nme]], envir = env)
        }

        message(vers, appendLF = !length(args))

        if (length(args)) {
          message("; ", paste0(names(args), ": ", args, collapse = ", "))
        }

        rlang::eval_tidy(setup, data = env)
        on.exit(rlang::eval_tidy(teardown, data = env))

        res <- bench::mark(
          iterations = nreps, check = FALSE, exprs = exprs,
          env = env, time_unit = "s"
        )

        arg <- lapply(c(list(version = vers), args), rep, nrow(res))
        ind <- colnames(res) == "expression"
        arg <- c(list(expression = names(res[[which(ind)]])), arg)

        cbind(arg, res[!ind])
      }

      if (length(args)) {
        do.call(rbind, lapply(args, eval_one, ...))
      } else {
        eval_one(args, ...)
      }
    }

    callr::r(eval_grid, c(args, vers), libpath = lib, show = TRUE)
  }

  exprs <- rlang::exprs(...)
  setup <- rlang::enquo(setup)
  teardown <- rlang::enquo(teardown)

  params <- expand.grid(grid, stringsAsFactors = FALSE)
  params <- split(params, seq_len(nrow(params)))

  res <- Map(
    eval_versions,
    vapply(versions, `[[`, character(1L), "lib"),
    names(versions),
    MoreArgs = list(
      list(params, setup, teardown, exprs, reps, seed, helpers, pkgs)
    )
  )

  bench::as_bench_mark(do.call(rbind, res))
}

bench_plot <- function(object, type = c(
                         "beeswarm", "jitter", "ridge",
                         "boxplot", "violin"
                       ),
                       check = FALSE, ref, new, threshold, ...) {
  within_thresh <- function(x, ref, new, thresh) {
    bounds <- quantile(x[[ref]], c(0.5 - thresh, 0.5 + thresh))
    val <- median(x[[new]])
    ifelse(
      val > bounds[2L], "slower", ifelse(val < bounds[1L], "faster", "same")
    )
  }

  labeller <- function(...) {
    sub_fun <- function(x) sub("^expression: ", "", x)
    lapply(ggplot2::label_both(...), sub_fun)
  }

  bench_cols <- function() {
    c(
      "min", "median", "itr/sec", "mem_alloc", "gc/sec", "n_itr", "n_gc",
      "total_time", "result", "memory", "time", "gc"
    )
  }

  extra_cols <- function(x) {
    setdiff(colnames(x), c("version", bench_cols(), c(
      "level0", "level1",
      "level2"
    ), "expression"))
  }

  type <- match.arg(type)

  if (type == "beeswarm" && !requireNamespace("ggbeeswarm", quietly = TRUE)) {
    stop("`ggbeeswarm` must be installed to use `type = \"beeswarm\"` option.")
  }

  res <- tidyr::unnest(object, c(time, gc))
  plt <- ggplot2::ggplot()

  params <- extra_cols(object)

  if (!isFALSE(check)) {
    grid <- object[, c("expression", params)]
    temp <- split(object, grid)
    temp <- Map(setNames, lapply(temp, `[[`, "time"),
      lapply(temp, `[[`, "version"),
      USE.NAMES = FALSE
    )

    grid <- cbind(
      do.call(rbind, lapply(split(grid, grid), unique)),
      `Median runtime` = vapply(
        temp, within_thresh, character(1L), ref, new,
        threshold
      )
    )

    plt <- plt +
      ggplot2::geom_rect(
        data = grid,
        ggplot2::aes(
          xmin = -Inf, xmax = Inf, ymin = -Inf, ymax = Inf,
          fill = `Median runtime`
        ),
        alpha = 0.05
      ) +
      ggplot2::scale_fill_manual(
        values = c(faster = "green", slower = "red", same = NA)
      )
  }

  plt <- switch(type,
    beeswarm = plt + ggbeeswarm::geom_quasirandom(
      data = res,
      ggplot2::aes_string("version", "time", color = "gc"),
      ...
    ) + ggplot2::coord_flip(),
    jitter = plt + ggplot2::geom_jitter(
      data = res,
      ggplot2::aes_string("version", "time", color = "gc"),
      ...
    ) + ggplot2::coord_flip(),
    ridge = plt + ggridges::geom_density_ridges(
      data = res,
      ggplot2::aes_string("time", "version"),
      ...
    ),
    boxplot = plt + ggplot2::geom_boxplot(
      data = res,
      ggplot2::aes_string("version", "time"),
      ...
    ) + ggplot2::coord_flip(),
    violin = plt + ggplot2::geom_violin(
      data = res,
      ggplot2::aes_string("version", "time"),
      ...
    ) + ggplot2::coord_flip()
  )

  if (length(params) == 0) {
    plt <- plt + ggplot2::facet_grid(
      rows = ggplot2::vars(expression),
      labeller = labeller
    )
  } else {
    plt <- plt + ggplot2::facet_grid(
      as.formula(paste("expression", "~", paste(params, collapse = " + "))),
      labeller = labeller, scales = "free_x"
    )
  }

  plt + ggplot2::labs(y = "Time [s]", color = "GC level") +
    ggplot2::theme_bw() +
    ggplot2::theme(
      axis.title.y = ggplot2::element_blank(),
      legend.position = "bottom"
    )
}
