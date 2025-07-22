duckdb_extension_load(inet
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-inet
    GIT_TAG eb2455703ca0665e69b9fd20fd1d8816c547cb49
    INCLUDE_DIR src/include
    TEST_DIR test/sql
    APPLY_PATCHES
    )
