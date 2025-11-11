duckdb_extension_load(httpfs
    LOAD TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 041a782b0b33495448a7eaa68973cf8c2174feb6
    INCLUDE_DIR src/include
    APPLY_PATCHES
)
