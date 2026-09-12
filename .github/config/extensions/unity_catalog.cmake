if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG 53f5da96a90082eb5798bcb31bfa7648a76523b6
            LOAD_TESTS
  )
endif()
