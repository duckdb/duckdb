if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG 48c0c8dae9c5e531d656f04822145a4436a5ee47
            SUBMODULES vortex
            APPLY_PATCHES
            LOAD_TESTS
            DONT_LINK
    )
endif()
