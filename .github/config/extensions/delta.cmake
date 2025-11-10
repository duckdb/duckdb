if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 0747c23791c6ad53dfc22f58dc73008d49d2a8ae
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
    )
endif()