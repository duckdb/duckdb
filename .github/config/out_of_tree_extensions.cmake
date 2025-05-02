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
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 22a0387e09389ee9148f58b3f8a71e7896b48093
    INCLUDE_DIR extension/httpfs/include
    APPLY_PATCHES
    )

################# AVRO
if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 712514078247b4b71f51d0c9eb862bec3928742c
    )
endif()

################## AWS
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            ### TODO: re-enable LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG e92e45b30ba17594b1101db22699a2244adfaeb1
            APPLY_PATCHES
            )
endif()

################# AZURE
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(azure
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-azure
            GIT_TAG 1593cb56745a51eb7d8415c1fd7d11a15f20f413
            APPLY_PATCHES
            )
endif()

################# DELTA
# MinGW build is not available, and our current manylinux ci does not have enough storage space to run the rust build
# for Delta
if (FALSE)
if (NOT MINGW AND NOT "${OS_NAME}" STREQUAL "linux" AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            ## TODO: GIT_TAG 90f244b3d572c1692867950b562df8183957b7a8
            GIT_TAG 6d626173e9efa6615c25eb08d979d1372100d5db
            APPLY_PATCHES
    )
endif()
endif()

################# EXCEL
duckdb_extension_load(excel
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-excel
    GIT_TAG b724b308b2b3a3c5644272cc84ec140fbcc7617d
    INCLUDE_DIR src/excel/include
    APPLY_PATCHES
    )

################# ICEBERG
# Windows tests for iceberg currently not working
IF (NOT WIN32)
    set(LOAD_ICEBERG_TESTS "LOAD_TESTS")
else ()
    set(LOAD_ICEBERG_TESTS "")
endif()

if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(iceberg
#            ${LOAD_ICEBERG_TESTS} TODO: re-enable once autoloading test is fixed
            GIT_URL https://github.com/duckdb/duckdb-iceberg
            GIT_TAG dd8617d636b6aa8bd83fa219cd238fc9823df4e3
            )
endif()

################# INET
duckdb_extension_load(inet
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-inet
    GIT_TAG a8b361ab5d43f6390d7cb48c9a9f0638e9581cf9
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
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG 98482ce5c144287f01e738275892cdb84ea9b5ce
            APPLY_PATCHES
            )
endif()

# mingw CI with all extensions at once is somehow not happy
if (NOT MINGW)
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 4be6065edc313a53ff2196ff79c11a0d5e249720
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
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG 66a5fa2448398379dc21c18308e3b95d42d84015
        APPLY_PATCHES
        )

duckdb_extension_load(sqlsmith
        DONT_LINK LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlsmith
        GIT_TAG e1eb0ae02a258f176d6e06b84c0d6c7a09c6b4da
        )

################# VSS
duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG ba199a7215b75e83821ece13f6b921ccfcebd6ac
        TEST_DIR test/sql
        APPLY_PATCHES
    )

################# MYSQL
if (NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG 93469fc39a317acf916627e0ddc724a076bf7302
            APPLY_PATCHES
            )
endif()

################# FTS
duckdb_extension_load(fts
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-fts
        GIT_TAG 3aa6a180b9c101d78070f5f7214c27552bb091c8
        TEST_DIR test/sql
)
