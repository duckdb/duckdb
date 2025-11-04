duckdb_extension_load(httpfs
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 8356a9017444f54018159718c8017ff7db4ea756
    INCLUDE_DIR src/include
    APPLY_PATCHES
)
