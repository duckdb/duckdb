if (NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(azure
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-azure
        GIT_TAG 8a5df75eaaaa9a75e6bb4596f374eb44ec35032d
  )
endif()
