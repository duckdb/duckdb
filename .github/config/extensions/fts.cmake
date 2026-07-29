duckdb_extension_load(fts
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-fts
        GIT_TAG 7f833ba49db334fa58b56d2f3cf66e3ae78ec201
        TEST_DIR test/sql
)
