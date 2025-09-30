duckdb_extension_load(httpfs
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 0518838dae609ab8e8ae66960ce982b839754075
    INCLUDE_DIR extension/httpfs/include
    APPLY_PATCHES
)
