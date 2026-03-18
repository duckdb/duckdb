if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG c4816d8233597118c765df9cb3eabb82466028f6
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
