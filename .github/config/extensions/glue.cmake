if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(glue
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws-glue
            GIT_TAG 4d76757a470f7c01007fef7077486be98c34b471
            SUBMODULES extension-ci-tools
            )
endif()
