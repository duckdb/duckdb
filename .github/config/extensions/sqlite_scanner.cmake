# Static linking on windows does not properly work due to symbol collision
duckdb_extension_load(sqlite_scanner
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG 4072b28f8ded96acf1fc7c31c2ad9a552eaf0f77
        APPLY_PATCHES
        SUBMODULES database-connector
        )
