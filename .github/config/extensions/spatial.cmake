if (NOT MINGW AND ${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG cf29a21952cdbecbe2961ea93d2e46cb74ac3e2b
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
