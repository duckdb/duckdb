if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 50de51108cc4d2c09a549e022fce4f74e17bf360
            SUBMODULES extension-ci-tools
    )
endif()