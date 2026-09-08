duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG 76675a648a900e3109da96e9ae245ba8cc9a45e8
        TEST_DIR test/sql
    )
