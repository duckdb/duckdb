duckdb_extension_load(fts
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-fts
        GIT_TAG 98efac320bb5fe57dd3103c1954e10774fb49e07
        TEST_DIR test/sql
        APPLY_PATCHES
)
