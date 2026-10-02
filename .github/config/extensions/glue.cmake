if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(glue
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws-glue
            GIT_TAG 26cebe53ddf4ef45e95d02f9fcd56433ab862fb1
            SUBMODULES extension-ci-tools
            )
endif()
