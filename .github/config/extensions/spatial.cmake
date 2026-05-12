if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG b5ea138c2517795cb2de62142d33bf7eeba29f7e
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
