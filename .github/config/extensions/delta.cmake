if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG e27cb002a34de7e1f8e033f3fccce0415032d98a
            SUBMODULES extension-ci-tools
    )
endif()