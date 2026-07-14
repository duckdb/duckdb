if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG 2a008b1734d563f46a1ff0af3a758f4fd844ea91
            APPLY_PATCHES
            LOAD_TESTS
            DONT_LINK
    )
endif()
