if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG f129b24b4ddd4d98cfc18f88be5a344a79040e7b
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
