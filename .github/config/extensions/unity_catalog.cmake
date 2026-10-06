if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG 7f6b833e19242af8cff883aa102f365729e17fb4
            LOAD_TESTS
  )
endif()
