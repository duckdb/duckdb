duckdb_extension_load(fts
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-fts
        GIT_TAG 39376623630a968154bef4e6930d12ad0b59d7fb
        TEST_DIR test/sql
)
