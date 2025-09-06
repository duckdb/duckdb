duckdb_extension_load(httpfs
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG d39fd75ed0544601f7b745c5de5fbeb916b14c1b
    INCLUDE_DIR extension/httpfs/include
)
