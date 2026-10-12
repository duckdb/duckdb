if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 6eb9bf905f86e71c2b4384a62ef0370736fb7940
            SUBMODULES extension-ci-tools
  )
endif()
