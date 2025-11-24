if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 48168a8ff954e9c3416f3e5affd201cf373b3250
            SUBMODULES extension-ci-tools
    )
endif()