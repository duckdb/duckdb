if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG b68b309d371dba936c5bb362980e559b7756b16d
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
