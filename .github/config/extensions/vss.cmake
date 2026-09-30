duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG c1430fd4bd2d96eb1fb2ddb4909f4818234390bc
        TEST_DIR test/sql
        APPLY_PATCHES
    )
