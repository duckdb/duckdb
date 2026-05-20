if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 6019a321b4e9a91cb0e81e29d8458d41c75ec3ba
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    APPLY_PATCHES
    )
endif()
