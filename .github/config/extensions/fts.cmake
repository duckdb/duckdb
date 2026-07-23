duckdb_extension_load(fts
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-fts
        GIT_TAG b6149a9e358460295ef4ccd9a450f61bddb1f3f3
        APPLY_PATCHES
        TEST_DIR test/sql
)
