duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG 590d53b7ae3ac125f726e1d4e003d7a8a0f62b84
        TEST_DIR test/sql
        APPLY_PATCHES
    )
