if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG b86c6ab223b53b445ec4c96e74f26dcb616d9c4f
            APPLY_PATCHES
            LOAD_TESTS
            DONT_LINK
    )
endif()
