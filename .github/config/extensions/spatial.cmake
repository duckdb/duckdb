if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 2f2668d211c0cf759f460403a108f24eb8b887e3
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
