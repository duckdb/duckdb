if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(lance
            GIT_URL https://github.com/lance-format/lance-duckdb
            GIT_TAG c6b3c7cfb0669ebf7ebf118b2e2ccf9a15d94779
            SUBMODULES extension-ci-tools
            LOAD_TESTS
    )
endif()
