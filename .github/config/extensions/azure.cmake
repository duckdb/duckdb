if (NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(azure
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-azure
        GIT_TAG abea9d0d90a1f585c3be41d07f725683fdaa0832
  )
endif()
