if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG ecd3884f2c94137481bb57641f7141dc7fbf520b
            APPLY_PATCHES
            LOAD_TESTS
            DONT_LINK
    )
endif()
