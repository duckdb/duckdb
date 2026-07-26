if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG e74cf88ebf64a6638de443025510d8269c31616a
            SUBMODULES extension-ci-tools
  )
endif()
