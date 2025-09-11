duckdb_extension_load(httpfs
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 7ee09f439c52f706414f271988de93f695ebe721
    INCLUDE_DIR extension/httpfs/include
    APPLY_PATCHES .github/patches/extensions/httpfs/fix.patch
)
