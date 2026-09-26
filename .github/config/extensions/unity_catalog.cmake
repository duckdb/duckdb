if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG 7d4d2f08f944244a3534ff0266e94bb491510bee
            LOAD_TESTS
  )
endif()
