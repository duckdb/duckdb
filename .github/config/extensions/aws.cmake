if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            ### TODO: re-enable LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 55bf3621fb7db254b473c94ce6360643ca38fac0
            )
endif()
