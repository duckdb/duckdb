if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG fd851475780ca064d9706a5025ea6e5d1d9d7e23
            LOAD_TESTS
  )
endif()
