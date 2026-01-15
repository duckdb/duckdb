duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG 9b25336989efdca9598ae90364ce13cc976f2f31
        TEST_DIR test/sql
    )
