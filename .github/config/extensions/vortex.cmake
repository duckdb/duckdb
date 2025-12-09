if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})

    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG dae36cd56988da2b47f06a1f63df0cfb47a97a50
            SUBMODULES vortex
            APPLY_PATCHES
            LOAD_TESTS
    )
endif()
