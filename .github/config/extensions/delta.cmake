if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG fa847248e163335bd1fc6bacf9c0d82e368e3426
            SUBMODULES extension-ci-tools
    )
endif()