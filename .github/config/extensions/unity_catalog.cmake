if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG 81e41aedbd0dd4bbdbea17290d2f967958984b6c
            LOAD_TESTS
  )
endif()
