if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 04270fe7bb4eb412765cce8afde0276a9d81d3ba
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
