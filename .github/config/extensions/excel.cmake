duckdb_extension_load(excel
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-excel
    GIT_TAG 9a5e88b3c9330449cbe0618100f68162969d14d4
    INCLUDE_DIR src/excel/include
    APPLY_PATCHES
    )
