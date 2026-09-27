if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG fa223642f3e8a4377e7fb6ce3a7f3f19767951e4
            LOAD_TESTS
  )
endif()
