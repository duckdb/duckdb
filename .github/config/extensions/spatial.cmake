if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 89ed6bb2e14928d81995d723cce88c18f6926fd2
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    APPLY_PATCHES
    )
endif()
