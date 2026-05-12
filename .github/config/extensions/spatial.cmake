if (${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 243c05d5514526fae6b111615dce1fdaf12d6ca8
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
