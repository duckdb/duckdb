#
# This config file holds all out-of-tree extension that are built with DuckDB's CI
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/out_of_tree_extensions.cmake make
#

################# ARROW
if (NOT WIN32)
    duckdb_extension_load(arrow
            LOAD_TESTS
            DONT_LINK
            GIT_URL https://github.com/duckdblabs/arrow
            GIT_TAG 56d24d4224df43cac0e071d71371385de5831129
            APPLY_PATCHES
            )
endif()

################# AWS
duckdb_extension_load(aws
        LOAD_TESTS
        GIT_URL https://github.com/duckdblabs/duckdb_aws
        GIT_TAG b769c8aab638bdbdc46c77fcb3c70412dd715d45
        )

################# AZURE
duckdb_extension_load(azure
        LOAD_TESTS
        GIT_URL https://github.com/duckdblabs/duckdb_azure
        GIT_TAG 1fe568d3eb3c8842118e395ba8031e2a8566daed
        )

################# ICEBERG
# Windows tests for iceberg currently not working
if (NOT WIN32)
    set(LOAD_ICEBERG_TESTS "LOAD_TESTS")
else ()
    set(LOAD_ICEBERG_TESTS "")
endif()

duckdb_extension_load(iceberg
        ${LOAD_ICEBERG_TESTS}
        GIT_URL https://github.com/duckdblabs/duckdb_iceberg
        GIT_TAG 6481aa4dd0ab9d724a8df28a1db66800561dd5f9
        APPLY_PATCHES
        )

################# POSTGRES_SCANNER
# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
duckdb_extension_load(postgres_scanner
        DONT_LINK
        GIT_URL https://github.com/duckdblabs/postgres_scanner
        GIT_TAG 828578442d18fb3acb53b08f4f54a0683217a2c8
        )

################# SPATIAL
duckdb_extension_load(spatial
        DONT_LINK
        LOAD_TESTS
        GIT_URL https://github.com/duckdblabs/duckdb_spatial.git
        GIT_TAG dc66594776fbe2f0a8a3af30af7f9f8626e6e215
        APPLY_PATCHES
        INCLUDE_DIR spatial/include
        TEST_DIR test/sql
        )

################# SQLITE_SCANNER
# Static linking on windows does not properly work due to symbol collision
if (WIN32)
    set(STATIC_LINK_SQLITE "DONT_LINK")
else ()
    set(STATIC_LINK_SQLITE "")
endif()

duckdb_extension_load(sqlite_scanner
        ${STATIC_LINK_SQLITE}
        LOAD_TESTS
        GIT_URL https://github.com/duckdblabs/sqlite_scanner
        GIT_TAG 9c38a30be2237456cdcd423d527b96c944158c77
        )

################# SUBSTRAIT
if (NOT WIN32)
    duckdb_extension_load(substrait
            LOAD_TESTS
            DONT_LINK
            GIT_URL https://github.com/duckdblabs/substrait
            GIT_TAG 0cd88fa8b240babe5316924e32fb68aaba408780
            )
endif()