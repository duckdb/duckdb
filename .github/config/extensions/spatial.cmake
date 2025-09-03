if (NOT MINGW AND ${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 35a3635d6e51151134226f38ffe1c4368a5a5292
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
