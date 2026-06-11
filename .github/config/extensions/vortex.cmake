if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG 27fa8e962dfa285cb04f399f3e41c052f8a73416
            SUBMODULES vortex
            APPLY_PATCHES
            LOAD_TESTS
            DONT_LINK
    )
endif()
