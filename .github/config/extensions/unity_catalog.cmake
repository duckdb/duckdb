if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG dbca44d4dcc67c196af5fd910f0f26ce56d4930e
            LOAD_TESTS
  )
endif()
