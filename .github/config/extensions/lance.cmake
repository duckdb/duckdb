if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(lance
            GIT_URL https://github.com/lance-format/lance-duckdb
            GIT_TAG 350060612087e1138ffa1bbb11a535013558241a
            SUBMODULES extension-ci-tools
            LOAD_TESTS
            DONT_LINK
    )
endif()
