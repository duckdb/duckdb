if (NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(azure
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-azure
        GIT_TAG 563589b2f24290a4dcdd4247eaedf2b544f9dbcd
  )
endif()
