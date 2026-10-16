if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG 289c93aa91127e6b5beed9e432d27ad77b3ccd6d
            LOAD_TESTS
  )
endif()
