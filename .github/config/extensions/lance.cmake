if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(lance
            GIT_URL https://github.com/lance-format/lance-duckdb
            GIT_TAG 291316938c4ab06dc9fec5b92820f496662ef24a
            SUBMODULES extension-ci-tools
            LOAD_TESTS
            DONT_LINK
    )
endif()
