if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG ee65aa33c23149af3ecfaf12b7aa011b006a9075
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
  )
endif()
