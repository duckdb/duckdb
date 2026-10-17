if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(lance
            GIT_URL https://github.com/lance-format/lance-duckdb
            GIT_TAG 86118537da79138e6ec5c7358f2384c7ae650ab0
            SUBMODULES extension-ci-tools
            LOAD_TESTS
    )
endif()
