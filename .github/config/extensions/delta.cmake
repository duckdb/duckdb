if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 53b2e694fb6d81b1e95b8274a3a56ea937419f99
            SUBMODULES extension-ci-tools
  )
endif()
