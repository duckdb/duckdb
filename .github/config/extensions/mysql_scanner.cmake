if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG 37006e53a58ddc31eeb96ff95c21f3196e27fcf2
            SUBMODULES database-connector
            )
endif()
