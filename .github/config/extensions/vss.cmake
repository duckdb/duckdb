duckdb_extension_load(vss
        LOAD_TESTS
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-vss
        GIT_TAG 35db414fcd805329e8e76db9987fdbadc6860979
        APPLY_PATCHES
        TEST_DIR test/sql
    )
