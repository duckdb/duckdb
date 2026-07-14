if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG f1c8c817e6dfcdf665dcd9aa556f0d37aae38742
            )
endif()
