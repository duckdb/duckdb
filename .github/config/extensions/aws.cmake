if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            ### TODO: re-enable LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG bc15d211f282d1d78fc0d9fda3d09957ba776423
            )
endif()
