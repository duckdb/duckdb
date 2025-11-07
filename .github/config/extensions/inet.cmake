duckdb_extension_load(inet
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-inet
    GIT_TAG fe7f60bb60245197680fb07ecd1629a1dc3d91c8
    INCLUDE_DIR src/include
    TEST_DIR test/sql
    )
