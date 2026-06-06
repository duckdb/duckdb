if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG 431196f9e196831b650cd1bdfa03281ee3a7b9d0
            SUBMODULES vortex
            APPLY_PATCHES
            LOAD_TESTS
    )
endif()
