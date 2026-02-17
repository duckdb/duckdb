duckdb_extension_load(inet
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-inet
    GIT_TAG bf675673d9ed8c08522863db4e40ccaa18c797e0
    INCLUDE_DIR src/include
    TEST_DIR test/sql
    )
