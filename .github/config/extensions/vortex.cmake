if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG 8a41ee6ebd9ada9ecefd36a3f8e9009f8d0ef4a9
            SUBMODULES vortex
            LOAD_TESTS
    )
endif()
