if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(odbc_scanner
            GIT_URL https://github.com/duckdb/odbc-scanner
            GIT_TAG a33698df40f95cc3eb339703dcb085497eabfca3
            APPLY_PATCHES
            )
endif()
