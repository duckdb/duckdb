if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG f1d33c76a5cd7706471784a93762d8b4363adcea
            )
endif()
