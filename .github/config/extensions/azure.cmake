if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(azure
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-azure
            GIT_TAG 60fec8598707bc9588e4db83ba798bad86d71bf1
            )
endif()
