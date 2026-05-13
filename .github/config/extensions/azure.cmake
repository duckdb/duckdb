if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(azure
        GIT_URL https://github.com/duckdb/duckdb-azure
        GIT_TAG 407270037ae87e7ab29a087b6d367e51e6970cf2
        LOAD_TESTS
  )
endif()
