if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG f1b8cf5bd32d88b3d72def3b3fcceba8640e84ae
            LOAD_TESTS
            )
endif()
