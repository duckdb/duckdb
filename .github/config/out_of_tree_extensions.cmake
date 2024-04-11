#
# This config file holds all out-of-tree extension that are built with DuckDB's CI
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/out_of_tree_extensions.cmake make
#
#  Note that many of these packages require vcpkg, and a merged manifest must be created to
#  compile multiple of them.
#
#  After setting up vcpkg, build using e.g. the following commands:
#  USE_MERGED_VCPKG_MANIFEST=1 BUILD_ALL_EXT=1 make extension_configuration
#  USE_MERGED_VCPKG_MANIFEST=1 BUILD_ALL_EXT=1 make debug
#
#  Make sure the VCPKG_TOOLCHAIN_PATH and VCPKG_TARGET_TRIPLET are set. For example:
#  VCPKG_TOOLCHAIN_PATH=~/vcpkg/scripts/buildsystems/vcpkg.cmake
#  VCPKG_TARGET_TRIPLET=arm64-osx

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
            GIT_TAG 4512a652479016d40d712f990cab9b9aab43d341
            APPLY_PATCHES
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
            APPLY_PATCHES
            )
endif()

################# POSTGRES_SCANNER
# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
if (NOT MINGW)
    duckdb_extension_load(postgres_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/postgres_scanner
            GIT_TAG 375710fd22a35107b2c28e744f787e1a93a99998
            )
endif()

################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb_spatial.git
    GIT_TAG 05c4ba01c500140287bf6946fb6910122e5c2acf
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
        GIT_TAG 70a4c411434290b4af704940987a324d84af0024
        APPLY_PATCHES
        )

################# SUBSTRAIT
if (NOT WIN32)
    duckdb_extension_load(substrait
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/substrait
            GIT_TAG 1116fb580edd3e26e675436dbdbdf4a0aa5e456e
            )
endif()


################# VSS
duckdb_extension_load(vss
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb_vss
        GIT_TAG f03fe75e16dd67e404d6325d4f901d6958504730
        TEST_DIR test/sql
        APPLY_PATCHES
    )