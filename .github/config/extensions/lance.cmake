if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(lance
            GIT_URL https://github.com/lance-format/lance-duckdb
            GIT_TAG cd592bc76dfeac28bc7d6344a5bf85c4d4c31405
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
            LOAD_TESTS
            DONT_LINK
    )
endif()
