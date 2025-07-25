duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG ccfa7c9c1f1f540fa7f433a93d32bed772aa44f4
        TEST_DIR test/sql
        APPLY_PATCHES
    )
