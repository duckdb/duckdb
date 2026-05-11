if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG ad54a347dba6a1da2167c716b2c67fdfb69cd499
            LOAD_TESTS
  )
endif()
