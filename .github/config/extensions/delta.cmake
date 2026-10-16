if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 0ebdebdf6ed9d2c8c6b17b077152ee1a00c4f2e4
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
  )
endif()
