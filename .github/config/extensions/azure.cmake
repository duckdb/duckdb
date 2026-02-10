if (NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(azure
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-azure
        GIT_TAG e2a654f618972bfbd959465bdecf20671d9224d7
  )
endif()
