if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG b593157d114ce57c044eb8adc48ab164f2e10c11
            LOAD_TESTS
  )
endif()
