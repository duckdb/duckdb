if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(glue
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws-glue
            GIT_TAG 24fa46ff3680ecb093673590911369d17fb6caf1
            SUBMODULES extension-ci-tools
            )
endif()
