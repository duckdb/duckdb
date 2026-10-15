if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG e130b416f1e424638567bc8e32ca994183997756
            SUBMODULES database-connector
            )
endif()
