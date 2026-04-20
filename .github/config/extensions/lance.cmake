#if (NOT MINGW AND NOT ${WASM_ENABLED})
#    duckdb_extension_load(lance
#            GIT_URL https://github.com/lance-format/lance-duckdb
#            GIT_TAG 4d9ecabf7dc8f3fd07b4ae4a77ff699018a8f4dc
#            SUBMODULES extension-ci-tools
#            LOAD_TESTS
#            DONT_LINK
#    )
#endif()