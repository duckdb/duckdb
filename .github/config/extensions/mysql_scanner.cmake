if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG e74ffaa8f85bb1296d308fbe04ca288abef7714f
            SUBMODULES database-connector
            )
endif()
