if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(lance
            GIT_URL https://github.com/lance-format/lance-duckdb
            GIT_TAG c62e9322d6847d1e3fca709d91e9286b60519fe2
            SUBMODULES extension-ci-tools
            LOAD_TESTS
            DONT_LINK
    )
endif()
