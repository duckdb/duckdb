if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(glue
            LOAD_TESTS
            APPLY_PATCHES
            GIT_URL https://github.com/duckdb/duckdb-aws-glue
            GIT_TAG 4f45064267da495aca32ae06599a16e0a604a47c
            SUBMODULES extension-ci-tools
            )
endif()
