if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(glue
            LOAD_TESTS
            APPLY_PATCHES
            GIT_URL https://github.com/duckdb/duckdb-aws-glue
            GIT_TAG 2811770780ab64b563c1d890b244f85025947aba
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
            )
endif()
