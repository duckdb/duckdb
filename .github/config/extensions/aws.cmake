if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            ### TODO: re-enable LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 7cacaf6dbdf9e7826df656c354bcaad987087df9
            )
endif()
