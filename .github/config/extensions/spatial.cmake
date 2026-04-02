if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG dc1996bfd16bd8614fb4ccb5895b3ee0dbd4298e
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
