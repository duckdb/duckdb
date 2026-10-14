if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG ff80df27a672eebc27aecd184176ef7bd5d01206
            LOAD_TESTS
  )
endif()
