if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG de46483edd4aab903a481db5332cbfa7970568f1
            APPLY_PATCHES
            )
endif()
