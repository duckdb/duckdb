if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 6515bb2560772956f9b74a18a47782f18ed4d827
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
    )
endif()