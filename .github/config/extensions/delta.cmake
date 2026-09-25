if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG ac61cfbcd20162167797704b81ae050062f485f9
            SUBMODULES extension-ci-tools
  )
endif()
