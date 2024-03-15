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
            GIT_TAG 9e10240da11f61ea7fbfe3fc9988ffe672ccd40f
            )
endif()

################## AWS
if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb_aws
            GIT_TAG f7b8729f1cce5ada5d4add70e1486de50763fb97
            )
endif()

################# AZURE
if (NOT MINGW)
    duckdb_extension_load(azure
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb_azure
            GIT_TAG 9e3e5b8addab5939b4ca4ac5f8997218a7228b8f
            )
endif()

################# ICEBERG
# Windows tests for iceberg currently not working
if (NOT WIN32)
    set(LOAD_ICEBERG_TESTS "LOAD_TESTS")
else ()
    set(LOAD_ICEBERG_TESTS "")
endif()

if (NOT MINGW)
    duckdb_extension_load(iceberg
            ${LOAD_ICEBERG_TESTS}
            GIT_URL https://github.com/duckdb/duckdb_iceberg
            GIT_TAG 7aa3d8e4cb7b513d35fdacfa28dc328771bc4047
            )
endif()

################# POSTGRES_SCANNER
# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
if (NOT MINGW)
    duckdb_extension_load(postgres_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/postgres_scanner
            GIT_TAG 883a8f1a8a487264855a5166f7df1f46ad386434
            APPLY_PATCHES
            )
endif()

################# SPATIAL
if (NOT MINGW)
    duckdb_extension_load(spatial
            DONT_LINK LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb_spatial.git
            GIT_TAG 05c4ba01c500140287bf6946fb6910122e5c2acf
            INCLUDE_DIR spatial/include
            TEST_DIR test/sql
            )
endif()

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
        GIT_TAG c1343464ef4397665b858db9c193d33fac591b1c
        )

################# SUBSTRAIT
if (NOT WIN32)
    duckdb_extension_load(substrait
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/substrait
            GIT_TAG 870bab8725d1123905296bfb1f35ce737434e0b3
            APPLY_PATCHES
            )
endif()
