if (NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG c87dfd2c2ec6a5ab2fb42a8f902124065834ff43
            LOAD_TESTS
  )
endif()
