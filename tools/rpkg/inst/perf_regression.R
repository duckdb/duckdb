
pkg_root <- function(...) {
  file.path(rprojroot::find_root(rprojroot::is_git_root), "tools", "rpkg", ...)
}

helpers <- pkg_root("inst", "helpers.R")

source(helpers)

temp_lib_dir <- dir_create()

versions <- list(
  master = list(repo = "duckdb/duckdb", branch = "master"),
  cpp11 = list(repo = "cynkra/duckdb", branch = "cpp11-4")
)

versions <- Map(`[[<-`, versions, "lib",
                dir_create(temp_lib_dir, names(versions)))

install_all(versions)

res <- bench_mark(
  versions,
  seed = 2022,
  helpers = helpers,
  pkgs = c("DBI"),
  iterations = 5,
  grid = list(nrow = c(1e3, 1e4)),
  setup = {

    con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    dat <- setup_data(nrow)
    tmp <- rand_string()

    dbWriteTable(con, tmp, dat)

    sql_all <- paste("SELECT * FROM", dbQuoteIdentifier(con, tmp))
  },
  write_from_df = write_table(con, dat),
  read_to_df = dbReadTable(con, tmp),
  sql_to_df = dbGetQuery(con, sql_all),
  teardown = {
    dbDisconnect(con, shutdown = TRUE)
  }
)
