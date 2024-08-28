#
# This is the DuckDB in-tree extension config as it will run on the CI
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/in_tree_extensions.cmake make
#

duckdb_extension_load(autocomplete EXTENSION_VERSION ${DUCKDB_NORMALIZED_VERSION})
duckdb_extension_load(fts EXTENSION_VERSION ${DUCKDB_NORMALIZED_VERSION})
duckdb_extension_load(httpfs EXTENSION_VERSION ${DUCKDB_NORMALIZED_VERSION})
duckdb_extension_load(icu EXTENSION_VERSION ${DUCKDB_NORMALIZED_VERSION})
duckdb_extension_load(json EXTENSION_VERSION ${DUCKDB_NORMALIZED_VERSION})
duckdb_extension_load(parquet EXTENSION_VERSION ${DUCKDB_NORMALIZED_VERSION})
duckdb_extension_load(tpcds EXTENSION_VERSION ${DUCKDB_NORMALIZED_VERSION})
duckdb_extension_load(tpch EXTENSION_VERSION ${DUCKDB_NORMALIZED_VERSION})

# Test extension for the upcoming C CAPI extensions
duckdb_extension_load(demo_capi DONT_LINK)
