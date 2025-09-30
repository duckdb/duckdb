if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            ### TODO: re-enable LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 2afb8de8716682de76ee8d5b57cdcec714c273f7
            )
endif()
