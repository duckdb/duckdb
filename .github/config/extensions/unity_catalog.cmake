if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG 02024095fe6eeed6f53e78dce43e83c7424abec2
            LOAD_TESTS
  )
endif()
