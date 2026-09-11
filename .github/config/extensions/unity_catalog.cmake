if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG 5be31beb33ed514630fe8aa41b4266c5733e75b5
            LOAD_TESTS
  )
endif()
