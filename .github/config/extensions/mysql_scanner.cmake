if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG 7fbd1ff39cb6260012955dfebcb3411202233234
            SUBMODULES database-connector
            )
endif()
