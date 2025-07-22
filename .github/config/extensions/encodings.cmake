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
