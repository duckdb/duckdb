if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG 1ad4f0b1fb12f661ff58fc91c2c9e7022591ea4b
            LOAD_TESTS
            )
endif()
