if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG 274cbfba9c1d3b01c9e94ea3433cd95605fbf0f0
            LOAD_TESTS
  )
endif()
