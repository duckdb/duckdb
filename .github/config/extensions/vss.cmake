duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG 4d07d6e3f6ce87013ae59a24b8d8c6740f2db307
        TEST_DIR test/sql
    )
