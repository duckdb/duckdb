#
# This is the DuckDB out-of-tree extension config as it will run on the CI
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/out_of_tree_extensions.cmake make
#

duckdb_extension_load(sqlite_scanner
    DONT_LINK
    GIT_URL https://github.com/duckdblabs/sqlite_scanner
    GIT_TAG 05dd50b3fc91ee4974964086efbc16be74fa115f
)
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
if (NOT WIN32)
    duckdb_extension_load(arrow
        DONT_LINK
        GIT_URL https://github.com/duckdblabs/arrow
        GIT_TAG 1a43a5513b96e4c6ffd92026775ffeb648e71dac
        APPLY_PATCHES
    )
    duckdb_extension_load(substrait
        DONT_LINK
        GIT_URL https://github.com/duckdblabs/substrait
        GIT_TAG 53da781310c9c680efb97576d33a5fde89a58870
    )
endif()