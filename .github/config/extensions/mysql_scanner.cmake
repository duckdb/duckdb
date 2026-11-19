if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG e04d8eef94c94e70c5d5663895610b1bb475d506
            SUBMODULES database-connector
            )
endif()
