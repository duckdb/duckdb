if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(odbc_scanner
            GIT_URL https://github.com/duckdb/odbc-scanner
            GIT_TAG 8ad367bea8e01b3bef42437b29354fbb91f00564
            )
endif()
