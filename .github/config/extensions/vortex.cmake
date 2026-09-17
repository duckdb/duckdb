if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG 9a8eb67805271e15deadcd5682a0a285d48e0578
            APPLY_PATCHES
            LOAD_TESTS
            DONT_LINK
    )
endif()
