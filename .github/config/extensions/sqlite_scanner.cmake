# Static linking on windows does not properly work due to symbol collision
duckdb_extension_load(sqlite_scanner
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG 748415f555201a4bff5278792fa1bf78a0af53ed
        SUBMODULES database-connector
        APPLY_PATCHES
        )
