if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 44ed8006114324bf260c3e1d63ac3d620c603acf
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
