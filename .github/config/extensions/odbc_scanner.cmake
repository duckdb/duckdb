if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(odbc_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/odbc-scanner
            GIT_TAG 7ce06c95c94b46a6984968439ca31ce968a2f473
            )
endif()
