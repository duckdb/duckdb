duckdb_extension_load(fts
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-fts
        GIT_TAG 7c83f1270ef00c389ea84ca2c2b61bd171bd73d1
        TEST_DIR test/sql
)
