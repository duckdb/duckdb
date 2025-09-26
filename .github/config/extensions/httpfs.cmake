duckdb_extension_load(httpfs
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 354d3f436a33f80f03a74419e76eb59459e19168
    INCLUDE_DIR extension/httpfs/include
    APPLY_PATCHES
)
