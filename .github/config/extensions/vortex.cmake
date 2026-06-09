if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG 0645af35a5f0d536656522f27eea13935de75e46
            SUBMODULES vortex
            APPLY_PATCHES
            LOAD_TESTS
            DONT_LINK
    )
endif()
