if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 45c40878601b54b4188b09e08732fe0d576ad222
            SUBMODULES extension-ci-tools
  )
endif()
