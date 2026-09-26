# Static linking on windows does not properly work due to symbol collision
duckdb_extension_load(sqlite_scanner
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG 9bc53cf6552461da57b2dad25f35090136633370
        SUBMODULES database-connector
        )
