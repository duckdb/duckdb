if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(odbc_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/odbc-scanner
            GIT_TAG 8a3266017af8a9abf14a49e2fd5df83d64eb5520
            )
endif()
