if (NOT MINGW AND NOT ${WASM_ENABLED} AND ${BUILD_COMPLETE_EXTENSION_SET})
duckdb_extension_load(encodings
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-encodings
        GIT_TAG aa46e099a8cfe1144c3ac9b5d95417af00459b6a
        TEST_DIR test/sql
)
endif()
