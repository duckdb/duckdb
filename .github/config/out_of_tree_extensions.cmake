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
if (NOT MINGW)
    duckdb_extension_load(arrow
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/arrow
            GIT_TAG fa40ec56236953aa5978cf1b4c65ed521424ce69
            )
endif()

################## AWS
if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb_aws
            GIT_TAG 42c78d3f99e1a188a2b178ea59e3c17907af4fb2
            )
endif()

################# AZURE
if (NOT MINGW)
    duckdb_extension_load(azure
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb_azure
            GIT_TAG 49b63dc8cd166952a0a34dfd54e6cfe5b823e05e
            APPLY_PATCHES
            )
endif()

################# DELTA
# MinGW build is not available, and our current manylinux ci does not have enough storage space to run the rust build
# for Delta
if (NOT MINGW AND NOT "${OS_NAME}" STREQUAL "linux" AND NOT WIN32)
    duckdb_extension_load(delta
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb_delta
            GIT_TAG 0b981978e8450a43f3b0bfdb84d382d61afbb1d0
    )
endif()

################# EXCEL
duckdb_extension_load(excel
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb_excel
    GIT_TAG 391b36dfdcc1c8c187529376efb4f7ddc9f57261
    INCLUDE_DIR extension/excel/include
    )

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
            GIT_TAG fe0471671924ce57308924c79f72efd18103e66b
            )
endif()

################# POSTGRES_SCANNER
# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
if (NOT MINGW)
    duckdb_extension_load(postgres_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/postgres_scanner
            GIT_TAG 120c0b12258d27758e240d61f5dc22a4238a14a6
            APPLY_PATCHES
            )
endif()

################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb_spatial.git
    GIT_TAG dbb9971c900c5888e3e3598af91de3b9b884aca6
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
        GIT_TAG 1ee5404c4b9a62a572c9076ebf4cabb7f4181ea6
        )

duckdb_extension_load(sqlsmith
        GIT_URL https://github.com/duckdb/duckdb_sqlsmith
        GIT_TAG 9729cade201987b22ec7fc4b700f6d34e1e4111f
        )

################# SUBSTRAIT
if (NOT WIN32)
    duckdb_extension_load(substrait
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/substrait
            GIT_TAG 237931391ebc7e6aee7aa81052fa1411f6c4128e
            APPLY_PATCHES
            )
endif()


################# VSS
duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb_vss
        GIT_TAG 9ff608f1edc6a1da2e41ee41b1ef42ba3169e71c
        TEST_DIR test/sql
        APPLY_PATCHES
    )

################# MYSQL
if (NOT MINGW)
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb_mysql
            GIT_TAG 6519c1953655b86f59884bb1b79f554b9fdf551b
            )
endif()
