if (NOT DEFINED VORTEX_ENABLED)
    set(VORTEX_ENABLED ON)
endif()
if (VORTEX_ENABLED AND NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG 318c7c84e63268a9e4ceee55a355133460304314
            APPLY_PATCHES
            LOAD_TESTS
    )
endif()
