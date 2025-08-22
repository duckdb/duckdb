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
    GIT_TAG e9bb99189d93c8ce6e0755907c38d283c963ae61
    INCLUDE_DIR extension/httpfs/include
    )

################## AVRO
if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 180e41e8ad13b8712d207785a6bca0aa39341040
    )
endif()

################## AWS
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            ### TODO: re-enable LOAD_TESTS
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG b73faadeaa4d2c880deb949771baf570f42fe8cc
            )
endif()

################# AZURE
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(azure
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-azure
            GIT_TAG dc66aa879c5e6a8cf60b2e9856e04e88a2e0c315
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
duckdb_extension_load(ducklake
    DONT_LINK
    GIT_URL https://github.com/duckdb/ducklake
    GIT_TAG 9cc2d903c51d360ff3fc6afb10cf38f8eac2e25b
)

################# EXCEL
duckdb_extension_load(excel
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
           ${LOAD_ICEBERG_TESTS}
            GIT_URL https://github.com/duckdb/duckdb-iceberg
            GIT_TAG 003a93fbb005a7fa2469400967a77db509595271
            )
endif()

################# INET
duckdb_extension_load(inet
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-inet
    GIT_TAG eb2455703ca0665e69b9fd20fd1d8816c547cb49
    INCLUDE_DIR src/include
    TEST_DIR test/sql
    )

################# POSTGRES_SCANNER
# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(postgres_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG c0411b950a0e80d232ba31f30bd484aebccca1b5
            APPLY_PATCHES
            )
endif()

# mingw CI with all extensions at once is somehow not happy
if (NOT MINGW AND ${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG bef22c4b109d235c2b5725ff9ccf191485d14fd8
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
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG ed38d770e0bbf1d5a6660ec1887cc5abef65be15
        )

duckdb_extension_load(sqlsmith
        DONT_LINK LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlsmith
        GIT_TAG 3b1ad2bd7234c1143b4a819517873f4b465168d2
        APPLY_PATCHES
        )

################# VSS
duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG bfb2cbad7d5e5658b0a3193454b3919832b31d11
        TEST_DIR test/sql
    )

################# MYSQL
if (NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG dc470684cc670d1a4185a1a408e5d4c61f5356e8
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

################# ENCODINGS
if (NOT MINGW AND NOT ${WASM_ENABLED} AND ${BUILD_COMPLETE_EXTENSION_SET})
duckdb_extension_load(encodings
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-encodings
        GIT_TAG dc3c206e237b517abcdd95ebe40d02dcd0f71084
        TEST_DIR test/sql
)
endif()
