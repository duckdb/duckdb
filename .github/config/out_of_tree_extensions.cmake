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
    APPLY_PATCHES
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 217ec8e04f6ed419c866a6d2496aa15aace4382f
    INCLUDE_DIR extension/httpfs/include
    )

################## AVRO
if (NOT MINGW)
    duckdb_extension_load(avro
            APPLY_PATCHES
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG ff766174cc6cc9c4ed93fc4b75871bcdffcc6e65
    )
endif()

################## AWS
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            APPLY_PATCHES
            ### TODO: re-enable LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 4f318ebd088e464266c511abe2f70bbdeee2fcd8
            )
endif()


################# AZURE
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(azure
            APPLY_PATCHES
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-azure
            GIT_TAG 8bac45ad4bb858a4b46e9eb3e03cf3a59e3f8df4
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

################ DUCKLAKE
#duckdb_extension_load(ducklake
#    DONT_LINK
#    GIT_URL https://github.com/duckdb/ducklake
#    GIT_TAG 9cc2d903c51d360ff3fc6afb10cf38f8eac2e25b
#)

################# EXCEL
duckdb_extension_load(excel
    APPLY_PATCHES
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-excel
    GIT_TAG cf00672f2d16685d9aefcca48c6a04d8c37d7015
    INCLUDE_DIR src/excel/include
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
            APPLY_PATCHES
#            ${LOAD_ICEBERG_TESTS} TODO: re-enable once autoloading test is fixed
            GIT_URL https://github.com/duckdb/duckdb-iceberg
            GIT_TAG 24dd874bee165661f6c3c79ee2a823f02941ed94
            )
endif()

################# INET
duckdb_extension_load(inet
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-inet
    GIT_TAG eb2455703ca0665e69b9fd20fd1d8816c547cb49
    INCLUDE_DIR src/include
    TEST_DIR test/sql
    APPLY_PATCHES
    )

################# POSTGRES_SCANNER
# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(postgres_scanner
            APPLY_PATCHES
            DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG 9b24967e06a4af0a3cd43f8372114202a400f5f5
            )
endif()

# mingw CI with all extensions at once is somehow not happy
if (NOT MINGW AND ${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 494d94912cc7ebcd8c43c9b6fc173a3e4142740f
    INCLUDE_DIR src/spatial
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
        GIT_TAG ed38d770e0bbf1d5a6660ec1887cc5abef65be15
        APPLY_PATCHES
        )

duckdb_extension_load(sqlsmith
        DONT_LINK LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlsmith
        GIT_TAG 06e8da8a95710c996fcd62f385962ccd36a363f6
        APPLY_PATCHES
        )

################# VSS
duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG ccfa7c9c1f1f540fa7f433a93d32bed772aa44f4
        TEST_DIR test/sql
        APPLY_PATCHES
    )

################# MYSQL
if (NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG b79ef7e2dde1f9253f9ad584883b029eba8d29a4
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
        APPLY_PATCHES
)

################# ENCODINGS
if (NOT ${WASM_ENABLED} AND ${BUILD_COMPLETE_EXTENSION_SET})
duckdb_extension_load(encodings
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-encodings
        GIT_TAG dc3c206e237b517abcdd95ebe40d02dcd0f71084
        TEST_DIR test/sql
        APPLY_PATCHES
)
endif()
