if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(azure
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-azure
            GIT_TAG 0709c0fa1cf67a668b58b1f06ff3e5fc1696e10a
            )
endif()
