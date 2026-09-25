duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG 2e566dac6e9f0064aea8b6e834357189183d5b0b
        TEST_DIR test/sql
    )
