if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG afb1a9d0995854324a796c11719a81d1ce18b92a
            SUBMODULES extension-ci-tools
    )
endif()
