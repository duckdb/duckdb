if (NOT MINGW AND NOT ${WASM_ENABLED} AND ${BUILD_COMPLETE_EXTENSION_SET})
duckdb_extension_load(encodings
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-encodings
        GIT_TAG 384c0ab5c0e915454a026466cbe830f2ea7937bf
        TEST_DIR test/sql
)
endif()
