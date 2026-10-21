if (${BUILD_COMPLETE_EXTENSION_SET} AND NOT ${WASM_ENABLED})
################# SPATIAL
duckdb_extension_load(spatial
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 4e066c17e2d1f24625baff4c62430d68ceb4e41f
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    APPLY_PATCHES
    )
endif()
