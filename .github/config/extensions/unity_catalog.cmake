if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG 26387d6c8f5bbbf5fad81c6e0008dd5710a9a714
            LOAD_TESTS
  )
endif()
