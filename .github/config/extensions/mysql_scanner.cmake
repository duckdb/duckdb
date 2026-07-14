if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG 7267164dab3409e943261aeee6ae32f1b00847a7
            SUBMODULES database-connector
            )
endif()
