duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG d796205e45ba38f894fb16bbbf1b89374d3be2e5
        TEST_DIR test/sql
    )
