# Static linking on windows does not properly work due to symbol collision
duckdb_extension_load(sqlite_scanner
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG 0598d6d808eb6ce2b2b98c18b9dee99933aafee4
        SUBMODULES database-connector
        APPLY_PATCHES
        )
