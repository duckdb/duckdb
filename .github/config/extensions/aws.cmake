if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            LOAD_TESTS APPLY_PATCHES
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG b2649e68341a9ee717588dd23f277904727ce793
            )
endif()
