if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 2a759e58c6e41f15f28323982c45b88e0d930d84
            APPLY_PATCHES
            )
endif()
