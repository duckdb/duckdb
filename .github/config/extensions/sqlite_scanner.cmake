# Static linking on windows does not properly work due to symbol collision
duckdb_extension_load(sqlite_scanner
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG 69f80a9f4b26bc5aecfd98e99d7a4359e601b295
        SUBMODULES database-connector
        APPLY_PATCHES
        )
