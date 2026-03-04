if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 380933c093f1af67515831e9b80fe8b690ee8d8a
            SUBMODULES extension-ci-tools
    )
endif()
