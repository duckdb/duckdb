duckdb_extension_load(vss
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG 9eba8b3dc41e819eafb693102b4fb4a2845934c1
        TEST_DIR test/sql
        APPLY_PATCHES
    )
