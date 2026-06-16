if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG d52a7ee8678a23a8e0f950e955b9ffa1df0c3395
            LOAD_TESTS
  )
endif()
