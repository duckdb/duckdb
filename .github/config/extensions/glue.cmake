if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(glue
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws-glue
            GIT_TAG 4e934691f33381cc683b308f8371436b0dcf2d13
            SUBMODULES extension-ci-tools
            )
endif()
