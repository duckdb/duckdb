duckdb_extension_load(vss
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG 6c5ae1b105892bb9f95386e783a5b7fc8b9adae3
        TEST_DIR test/sql
        APPLY_PATCHES
    )
