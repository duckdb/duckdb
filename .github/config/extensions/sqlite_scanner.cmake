# Static linking on windows does not properly work due to symbol collision
duckdb_extension_load(sqlite_scanner
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG b73b6391d41c2465a839271cfe2f23e18fc2cb08
        SUBMODULES database-connector
        APPLY_PATCHES
        )
