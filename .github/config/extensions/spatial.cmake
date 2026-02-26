if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 03611be471d55b5390239098f8d441591bb8ae4f
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
