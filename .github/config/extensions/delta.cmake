if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG abf3891fd7013222f3fe7d65a54d19afd1477c37
            SUBMODULES extension-ci-tools
  )
endif()
