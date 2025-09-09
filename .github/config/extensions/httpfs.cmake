duckdb_extension_load(httpfs
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG c31f9e922a5cc99c5854fd9529954cb364afd1f1
    INCLUDE_DIR extension/httpfs/include
    APPLY_PATCHES
)
