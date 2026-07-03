if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG e37b1b48f526c605b0d38e515ab099049b965f7d
            LOAD_TESTS
  )
endif()
