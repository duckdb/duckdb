if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG d0b5bae4d907694e776c97b57de6edfb662c6c25
            SUBMODULES vortex
            APPLY_PATCHES
            LOAD_TESTS
            DONT_LINK
    )
endif()
