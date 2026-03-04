duckdb_extension_load(fts
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-fts
        GIT_TAG 6814ec9a7d5fd63500176507262b0dbf7cea0095
        TEST_DIR test/sql
)
