
pkg_root <- function(...) {
  file.path(rprojroot::find_root(rprojroot::is_git_root), "tools", "rpkg", ...)
}

helpers <- pkg_root("tests", "regression", "helpers.R")

source(helpers)

temp_lib_dir <- dir_create()

versions <- list(
  master = list(repo = "duckdb/duckdb", branch = "master"),
  release = list(repo = "duckdb/duckdb", ref = "v0.3.1")
)

versions <- Map(
  `[[<-`, versions, "lib",
  dir_create(temp_lib_dir, names(versions))
)

install_all(versions, update_deps = TRUE)

res <- bench_mark(
  versions,
  seed = 2022,
  helpers = helpers,
  pkgs = c("DBI", "duckdb", "arrow"),
  reps = 50,
  grid = list(nrow = c(1e3, 1e5, 1e7)),
  setup = {
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    dat <- setup_data(nrow)
    arw <- InMemoryDataset$create(dat)

    tmp_tbl <- rand_string()
    dbWriteTable(con, tmp_tbl, dat)

    tmp_df <- rand_string()
    duckdb_register(con, tmp_df, dat)

    tmp_arw <- rand_string()
    duckdb_register_arrow(con, tmp_arw, arw)
  },
  write_from_df = write_df(con, dat),
  register_df = register_df(con, dat),
  register_arrow = register_arrow(con, dat),
  read_from_tbl = dbReadTable(con, tmp_tbl),
  select_from_tbl = select_some(con, tmp_tbl),
  read_from_df = dbReadTable(con, tmp_df),
  select_from_df = select_some(con, tmp_df),
  read_from_arw = dbReadTable(con, tmp_arw),
  select_from_arw = select_some(con, tmp_arw),
  teardown = {
    dbDisconnect(con, shutdown = TRUE)
  }
)

bench_plot(res, check = TRUE, ref = "release", new = "master", threshold = 0.15)
ggplot2::ggsave("perf_reg.png", width = 9, height = 11)
