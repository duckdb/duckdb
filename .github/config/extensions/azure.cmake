
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(azure
            APPLY_PATCHES
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-azure
            GIT_TAG 8bac45ad4bb858a4b46e9eb3e03cf3a59e3f8df4
            )
endif()