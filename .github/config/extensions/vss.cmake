duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG b833341c8737fd3f3558c7720cc575ae8fc82598
        TEST_DIR test/sql
    )
