if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG 3327c7c8ae2db5e681ed77dcf6298ba43ca1c592
            SUBMODULES vortex
            LOAD_TESTS
    )
endif()
