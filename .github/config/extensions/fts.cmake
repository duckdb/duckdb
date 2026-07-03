duckdb_extension_load(fts
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-fts
        GIT_TAG 69c44bed3ceae0b9dcf2c7888314f37fb3ea8ca3
        APPLY_PATCHES
        TEST_DIR test/sql
)
