# Static linking on windows does not properly work due to symbol collision
duckdb_extension_load(sqlite_scanner
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG 79dd1203bc7a7ce4e091b61ce2670229b9cb71ad
        SUBMODULES database-connector
        )
