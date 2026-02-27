if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(lance
            GIT_URL https://github.com/lance-format/lance-duckdb
            GIT_TAG 892b224af0f4a5508e5f3c46f1b8292155880258
            LOAD_TESTS
    )
endif()
