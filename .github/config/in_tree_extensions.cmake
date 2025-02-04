#
# This is the DuckDB in-tree extension config as it will run on the CI
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/in_tree_extensions.cmake make
#

duckdb_extension_load(autocomplete)
duckdb_extension_load(core_functions)
duckdb_extension_load(icu)
duckdb_extension_load(json)
duckdb_extension_load(parquet)
duckdb_extension_load(tpcds)
duckdb_extension_load(tpch)

# Test extension for the upcoming C CAPI extensions
duckdb_extension_load(demo_capi DONT_LINK)
