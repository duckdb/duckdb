if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 7eaa663835aa5c627cb2b6f50ffcc5487ad973f3
            APPLY_PATCHES
            )
endif()
