################################################################################
# Python DuckDB extension config
################################################################################
#
# This is the default extension configuration for Python builds. Basically it means that all these extensions are
# "baked in" to the python binaries. Note that the configuration here is only when building Python using the main
# CMakeLists.txt file with the `BUILD_PYTHON` variable.
# TODO: unify this by making setup.py also use this configuration, making this the config for all python builds
duckdb_extension_load(json)
duckdb_extension_load(fts)
duckdb_extension_load(tpcds)
duckdb_extension_load(tpch)
duckdb_extension_load(parquet)
duckdb_extension_load(icu)