if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG bbeb8e68c350c3e20af759ebf583d7e29b61e826
            APPLY_PATCHES
            LOAD_TESTS
            DONT_LINK
    )
endif()
