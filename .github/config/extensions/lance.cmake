if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(lance
            GIT_URL https://github.com/lance-format/lance-duckdb
            GIT_TAG 3633381331446328924e8a1d75d8bbfec960952e
            LOAD_TESTS
    )
endif()
