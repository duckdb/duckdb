if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG c5d418c1779f3cb5226cddccd3850502dd6801fc
            SUBMODULES vortex
            APPLY_PATCHES
            LOAD_TESTS
    )
endif()
