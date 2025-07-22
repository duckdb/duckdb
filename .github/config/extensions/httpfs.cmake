duckdb_extension_load(httpfs
    APPLY_PATCHES
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 217ec8e04f6ed419c866a6d2496aa15aace4382f
    INCLUDE_DIR extension/httpfs/include
    )