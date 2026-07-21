if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(lance
            GIT_URL https://github.com/lance-format/lance-duckdb
            GIT_TAG 2f167ea1aa8b1201c89d53740b84deb00aff680e
            SUBMODULES extension-ci-tools
            LOAD_TESTS
            DONT_LINK
    )
endif()
