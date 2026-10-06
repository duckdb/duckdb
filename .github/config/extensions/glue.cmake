if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(glue
            LOAD_TESTS
            APPLY_PATCHES
            GIT_URL https://github.com/duckdb/duckdb-aws-glue
            GIT_TAG bcef110077d9dbacb3491642feaeae273b86e135
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
            )
endif()
