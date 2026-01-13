duckdb_extension_load(fts
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-fts
        GIT_TAG af6ebee119fd8dd4089367df08479c177be8960a
        TEST_DIR test/sql
)
