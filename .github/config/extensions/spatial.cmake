if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 91a6649a3f92217201a9e7a15b11879e3e0ae358
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
