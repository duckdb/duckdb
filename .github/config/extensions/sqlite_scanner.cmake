# Static linking on windows does not properly work due to symbol collision
duckdb_extension_load(sqlite_scanner
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG 9c5f9d3d886dcbda537afceea05e16509b0ddf44
        SUBMODULES database-connector
        APPLY_PATCHES
        )
