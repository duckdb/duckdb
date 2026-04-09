if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(lance
            GIT_URL https://github.com/lance-format/lance-duckdb
            GIT_TAG 1b4ef6865a8a1056c587e763366bcfda02939716
            SUBMODULES extension-ci-tools
            LOAD_TESTS
            DONT_LINK
    )
endif()
