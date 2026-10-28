if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(odbc_scanner
            GIT_URL https://github.com/duckdb/odbc-scanner
            GIT_TAG c0ab7fb98603c8c8840707068fe97f93c309f3f5
            )
endif()
