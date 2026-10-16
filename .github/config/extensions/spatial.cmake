if (${BUILD_COMPLETE_EXTENSION_SET} AND NOT ${WASM_ENABLED})
################# SPATIAL
duckdb_extension_load(spatial
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG cfca7f2648e6562934b70ec0109cf286bbf34fd7
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    APPLY_PATCHES
    )
endif()
