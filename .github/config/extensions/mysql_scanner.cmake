if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG accd6a798c8bd82a5e741f028d2c62608a093e29
            SUBMODULES database-connector
            APPLY_PATCHES
            )
endif()
