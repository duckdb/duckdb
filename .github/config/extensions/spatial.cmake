if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 686950e980a0629c5ffbc788b498681e2b06e75e
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
