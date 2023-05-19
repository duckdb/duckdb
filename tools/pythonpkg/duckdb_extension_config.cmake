################################################################################
# Python DuckDB extension config
################################################################################
#
# This is the default extension configuration for Python builds. Basically it
# means that all these extensions are "baked in" to the python binaries
duckdb_extension_load(json)
duckdb_extension_load(fts)
duckdb_extension_load(visualizer)
duckdb_extension_load(tpcds)
duckdb_extension_load(tpch)
duckdb_extension_load(parquet)
duckdb_extension_load(icu)