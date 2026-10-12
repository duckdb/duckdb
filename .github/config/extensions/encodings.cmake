if (NOT MINGW AND NOT ${WASM_ENABLED} AND ${BUILD_COMPLETE_EXTENSION_SET})
duckdb_extension_load(encodings
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-encodings
        GIT_TAG 60086d92fda614717db205ee4981da699ed39f9a
        TEST_DIR test/sql
)
endif()
