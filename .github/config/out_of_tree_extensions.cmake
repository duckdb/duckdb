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

################# HTTPFS
duckdb_extension_load(httpfs
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb_httpfs
    GIT_TAG 92867fce5a6e90b9a6803fab2b196c00cc11e46c
    INCLUDE_DIR extension/httpfs/include
    )

################# ARROW
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(arrow
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/arrow
            GIT_TAG c50862c82c065096722745631f4230832a3a04e8
            APPLY_PATCHES
            )
endif()

################## AWS
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb_aws
            GIT_TAG f743d4b3c2faecda15498d0219a1727ad6d62b5b
            APPLY_PATCHES
            )
endif()

################# AZURE
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(azure
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb_azure
            GIT_TAG 88011ee6ef66f223badc9beb04d4723651ac6623
            )
endif()

################# DELTA
# MinGW build is not available, and our current manylinux ci does not have enough storage space to run the rust build
# for Delta
if (NOT MINGW AND NOT "${OS_NAME}" STREQUAL "linux" AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb_delta
            GIT_TAG b7333c0143e101c720117d564651e693b317bb31
            APPLY_PATCHES
    )
endif()

################# EXCEL
duckdb_extension_load(excel
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb_excel
    GIT_TAG e243577956f36a898d5485189e5288839c2c4b73
    INCLUDE_DIR src/excel/include
    )

################# ICEBERG
# Windows tests for iceberg currently not working
#if (NOT WIN32)
#    set(LOAD_ICEBERG_TESTS "LOAD_TESTS")
#else ()
#    set(LOAD_ICEBERG_TESTS "")
#endif()
#
#if (NOT MINGW AND NOT ${WASM_ENABLED})
#    duckdb_extension_load(iceberg
#            ${LOAD_ICEBERG_TESTS}
#            GIT_URL https://github.com/duckdb/duckdb_iceberg
#            GIT_TAG d62d91d8a089371c4d1862a88f2e62a97bc2af3a
#            APPLY_PATCHES
#            )
#endif()

################# INET
duckdb_extension_load(inet
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb_inet
    GIT_TAG 51d7ad789f34eecb36a2071bac5aef0e12747d70
    INCLUDE_DIR src/include
    TEST_DIR test/sql
    APPLY_PATCHES
    )

################# POSTGRES_SCANNER
# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(postgres_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/postgres_scanner
            GIT_TAG 03eaed75f0ec5500609b7a97aa05468493b229d1
            APPLY_PATCHES
            )
endif()

# mingw CI with all extensions at once is somehow not happy
if (NOT MINGW)
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb_spatial.git
    GIT_TAG a60aa3733741a99c49baaf33390c0f7c8a9598a3
    INCLUDE_DIR spatial/include
    TEST_DIR test/sql
    APPLY_PATCHES
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
        GIT_TAG d5d62657702d33cb44a46cddc7ffc4b67bf7e961
        APPLY_PATCHES
        )

duckdb_extension_load(sqlsmith
        DONT_LINK LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb_sqlsmith
        GIT_TAG b13723fe701f1e38d2cd65b3b6eb587c6553a251
        )

################# VSS
duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb_vss
        GIT_TAG bae5b0653b18bbd05840d1773a49dc4a1165831f
        TEST_DIR test/sql
        APPLY_PATCHES
    )

################# MYSQL
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb_mysql
            GIT_TAG f2a15013fb4559e1591e977c1c023aa0a369c6f3
            APPLY_PATCHES
            )
endif()

################# FTS
duckdb_extension_load(fts
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb_fts
        GIT_TAG 0477abaf2484aa7b9aabf8ace9dc0bde80a15554
        TEST_DIR test/sql
)
