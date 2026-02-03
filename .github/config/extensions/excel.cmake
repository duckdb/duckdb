duckdb_extension_load(excel
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-excel
    GIT_TAG 9421a2d75bd7544336caa73e5f9e6063cc7f6992
    INCLUDE_DIR src/excel/include
    APPLY_PATCHES
    )
