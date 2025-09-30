if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(azure
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-azure
            GIT_TAG 5e458fcc466d2bc421922b11f4316564e3017800
            )
endif()
