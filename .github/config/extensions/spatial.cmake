if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 61ede09becdc06c4a3d0df1c1e8361be478142be
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
