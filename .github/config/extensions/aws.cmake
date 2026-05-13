if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 4bb2f0333ae38bc5ba0d61e503d3a2bbf240582d
            )
endif()
