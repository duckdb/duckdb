if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 77b0f303b7918f1539947833b1ced1eab8e825fc
            SUBMODULES extension-ci-tools
    )
endif()