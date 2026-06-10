if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(odbc_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/odbc-scanner
            GIT_TAG 274a3307341dcafd62471c09b45c5d858d6c95cc
            )
endif()
