if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG 0e3c49a933d9e5b258a3a12ec3cf4c6a984303a5
            SUBMODULES database-connector
            )
endif()
