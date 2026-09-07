if (NOT MINGW AND NOT ${WASM_ENABLED} AND ${BUILD_COMPLETE_EXTENSION_SET})
duckdb_extension_load(encodings
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-encodings
        GIT_TAG f3e4d03ecf18406ef86fe5833a7ad55d8f93520f
        TEST_DIR test/sql
        APPLY_PATCHES
)
endif()
