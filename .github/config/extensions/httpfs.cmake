duckdb_extension_load(httpfs
    APPLY_PATCHES
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 6f22256ac7bebf8e526ef3271e89d177184ec2ed
    INCLUDE_DIR extension/httpfs/include
    )
