if (NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG ff806ac8fcf3387d037d571c8cc9d571e3fac6ad
            LOAD_TESTS
            )
endif()
