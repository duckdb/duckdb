#
# This is the DuckDB out-of-tree extension config as it will run on the CI
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/out_of_tree_extensions.cmake make
#
duckdb_extension_load(postgres_scanner
    DONT_LINK
    GIT_URL https://github.com/duckdblabs/postgres_scanner
    GIT_TAG cd043b49cdc9e0d3752535b8333c9433e1007a48
)
duckdb_extension_load(spatial
    DONT_LINK
    GIT_URL https://github.com/duckdblabs/duckdb_spatial.git
    GIT_TAG f577b9441793f9170403e489f5d3587e023a945f
    INCLUDE_DIR spatial/include
    TEST_DIR spatial/test/sql
    APPLY_PATCHES
)