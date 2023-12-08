#
# This config file holds all out-of-tree extension that are built with DuckDB's CI
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/out_of_tree_extensions.cmake make
#

################# ARROW
if (NOT WIN32)
    duckdb_extension_load(arrow
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/arrow
            GIT_TAG 1b5b9649d28cd7f79496fb3f2e4dd7b03bf90ac5
            APPLY_PATCHES
            )
endif()

################# AWS
duckdb_extension_load(aws
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb_aws
        GIT_TAG af729d027e57175c5496a2d7dfef68833e6d6cd3
        )

################# AZURE
duckdb_extension_load(azure
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb_azure
        GIT_TAG 10d4cf6a0ed00ea8aecb9bf1433fdfff166e6c44
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
        GIT_URL https://github.com/duckdb/duckdb_iceberg
        GIT_TAG d8be56a293331a94d8e8d426b37d4593fc7dbd82
        )

################# POSTGRES_SCANNER
# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
duckdb_extension_load(postgres_scanner
        DONT_LINK
        GIT_URL https://github.com/duckdb/postgres_scanner
        GIT_TAG 8c3e9624ee1d32f317e18136056dfca9fb97ee67
        APPLY_PATCHES
        )

################# SPATIAL
duckdb_extension_load(spatial
        DONT_LINK LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb_spatial.git
        GIT_TAG a86c504d60e0f4400564c0f2d633547f39feef2c
        INCLUDE_DIR spatial/include
        TEST_DIR test/sql
        APPLY_PATCHES
        )

################# SQLITE_SCANNER
# Static linking on windows does not properly work due to symbol collision
if (WIN32)
    set(STATIC_LINK_SQLITE "DONT_LINK")
else ()
    set(STATIC_LINK_SQLITE "")
endif()

duckdb_extension_load(sqlite_scanner
        ${STATIC_LINK_SQLITE} LOAD_TESTS
        GIT_URL https://github.com/duckdb/sqlite_scanner
        GIT_TAG ef91604503e5c9ef0cf89db4a29f7c97e7ba1fb5
        APPLY_PATCHES
        )

################# SUBSTRAIT
if (NOT WIN32)
    duckdb_extension_load(substrait
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/substrait
            GIT_TAG 5d621b1d7d16fe86f8b1930870c8e6bf05bcb92a
            APPLY_PATCHES
            )
endif()
