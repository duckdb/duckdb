if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG e687032816845fd2c8cebcbda7441308571ca509
            SUBMODULES extension-ci-tools
  )
endif()
