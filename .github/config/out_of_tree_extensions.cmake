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
        GIT_TAG 348ae2625de86ab760f80a43eb76e4441cd01354
        )

################# AZURE
duckdb_extension_load(azure
        LOAD_TESTS
        GIT_URL https://github.com/duckdblabs/duckdb_azure
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
        GIT_URL https://github.com/duckdblabs/duckdb_iceberg
        GIT_TAG ca70abdbd1e446b5e58b3dd1b3b4fcc072345445
        )

################# POSTGRES_SCANNER
# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
duckdb_extension_load(postgres_scanner
        DONT_LINK
        GIT_URL https://github.com/duckdblabs/postgres_scanner
        GIT_TAG 844f46536b5d5f9e65b57b7ff92f4ce3346e2829
        )

################# SPATIAL
duckdb_extension_load(spatial
        DONT_LINK LOAD_TESTS
        GIT_URL https://github.com/duckdblabs/duckdb_spatial.git
        GIT_TAG 36e5a126976ac3b66716893360ef7e6295707082
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
        GIT_TAG 3443b2999ae1e68a108568fd32145705237a5760
        )

################# SUBSTRAIT
if (NOT WIN32)
    duckdb_extension_load(substrait
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdblabs/substrait
            GIT_TAG 5d621b1d7d16fe86f8b1930870c8e6bf05bcb92a
            )
endif()
