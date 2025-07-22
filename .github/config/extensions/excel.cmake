
duckdb_extension_load(excel
    APPLY_PATCHES
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-excel
    GIT_TAG cf00672f2d16685d9aefcca48c6a04d8c37d7015
    INCLUDE_DIR src/excel/include
    )