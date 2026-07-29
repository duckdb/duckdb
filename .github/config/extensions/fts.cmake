duckdb_extension_load(fts
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-fts
        GIT_TAG 67b8b0b5cd4dda27d97acc6a3e4fe938d95f4a0f
        TEST_DIR test/sql
)
