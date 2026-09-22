if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG b88d791484e16b5b16bd948c85eaf9ab4334ed63
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
