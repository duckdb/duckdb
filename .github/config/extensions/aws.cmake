if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG ee212dff67cd055bf55dc92e1b9fae2dab4760dc
            )
endif()
