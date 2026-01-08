if (NOT MINGW AND NOT ${WASM_ENABLED} AND ${BUILD_COMPLETE_EXTENSION_SET})
duckdb_extension_load(encodings
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-encodings
        GIT_TAG 7b7f8f08e4d01df8d733cde6ac1018402f5c7472
        TEST_DIR test/sql
)
endif()
