Sys.setenv(DUCKDB_R_DEBUG = "1")
pkgload::load_all()
con = dbConnect(duckdb::duckdb())
