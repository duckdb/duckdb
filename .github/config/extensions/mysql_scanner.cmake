if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG 1b7a31b95b0f3b2e9c1b64de6628ef438bec83a5
            SUBMODULES database-connector
            )
endif()
