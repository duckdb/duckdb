duckdb_extension_load(excel
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-excel
    GIT_TAG cc89a4c5da5f9acdf62bb70c5614a5844526fd83
    INCLUDE_DIR src/excel/include
    APPLY_PATCHES
    )
