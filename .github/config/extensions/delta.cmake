if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 98e55e001258ac06e99091b354417561d24da35b
            SUBMODULES extension-ci-tools
    )
endif()