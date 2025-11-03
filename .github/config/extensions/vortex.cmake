if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})

    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG 9ea698117199440f62a7cf1673afb647dc6437c7
            SUBMODULES vortex
            APPLY_PATCHES
            LOAD_TESTS
    )
endif()
