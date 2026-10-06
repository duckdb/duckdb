# Static linking on windows does not properly work due to symbol collision
duckdb_extension_load(sqlite_scanner
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG ca01682537b9bedfe785083547a1a8c253ced658
        SUBMODULES database-connector
        APPLY_PATCHES
        )
