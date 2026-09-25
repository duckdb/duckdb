duckdb_extension_load(inet
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-inet
    GIT_TAG 7c0a9c478d356673fa89aa869868365fba2eb7fb
    INCLUDE_DIR src/include
    TEST_DIR test/sql
    APPLY_PATCHES
    )
