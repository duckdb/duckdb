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
            GIT_URL https://github.com/duckdblabs/arrow
            GIT_TAG 1b5b9649d28cd7f79496fb3f2e4dd7b03bf90ac5
            )
endif()

################# AWS
duckdb_extension_load(aws
        LOAD_TESTS
        GIT_URL https://github.com/duckdblabs/duckdb_aws
        GIT_TAG a1f65419dfbc23e8099fbdd1a8a13bfda425165d
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
        GIT_TAG 51ba9564859698c29db4165f17143a2f6af2bb18
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
        DONT_LINK LOAD_TESTS
        GIT_URL https://github.com/duckdblabs/duckdb_spatial.git
        GIT_TAG 2f55d5d64bad9b5fc7ce67e4bcf52617ee31b865
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
        ${STATIC_LINK_SQLITE} LOAD_TESTS
        GIT_URL https://github.com/duckdblabs/sqlite_scanner
        GIT_TAG 9c38a30be2237456cdcd423d527b96c944158c77
        APPLY_PATCHES
        )

################# SUBSTRAIT
if (NOT WIN32)
    duckdb_extension_load(substrait
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdblabs/substrait
            GIT_TAG 5d621b1d7d16fe86f8b1930870c8e6bf05bcb92a
            )
endif()
