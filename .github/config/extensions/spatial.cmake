if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG eb1ca60832a6dbdf09ddc6138a4640781af311f4
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
