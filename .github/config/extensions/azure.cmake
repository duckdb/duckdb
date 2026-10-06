if (NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(azure
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-azure
        GIT_TAG 0ce9955c448c4c4d1536b067bd505eca6f96a4a0
  )
endif()
