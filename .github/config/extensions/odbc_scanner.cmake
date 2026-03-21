if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(odbc_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/odbc-scanner
            GIT_TAG 2f70a4b34c7b5aba795caa3300b208806cde24c9
            )
endif()
