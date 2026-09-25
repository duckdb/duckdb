if (${BUILD_COMPLETE_EXTENSION_SET} AND NOT ${WASM_ENABLED})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 4295b9b9a1b5a16b0a6c07880356ff3c4a21e676
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    APPLY_PATCHES
    )
endif()
