duckdb_extension_load(httpfs
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 6c187d86edde066adb2c51a411f3b6020da79e12
    APPLY_PATCHES
    INCLUDE_DIR src/include
)
