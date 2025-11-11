if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG d83faf88cd7e4d4cca4edd056a530382c5018654
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    APPLY_PATCHES
    )
endif()
