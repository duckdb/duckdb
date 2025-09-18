duckdb_extension_load(inet
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-inet
    GIT_TAG f6a2a14f061d2dfccdb4283800b55fef3fcbb128
    INCLUDE_DIR src/include
    TEST_DIR test/sql
    )
