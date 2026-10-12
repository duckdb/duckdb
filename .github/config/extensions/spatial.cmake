if (${BUILD_COMPLETE_EXTENSION_SET} AND NOT ${WASM_ENABLED})
################# SPATIAL
duckdb_extension_load(spatial
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 9926b5806a34363a7d58f6e0edbee272534712bc
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
