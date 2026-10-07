if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 5f6baaa4463237a13efda1192e2d92c206c6fab9
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
  )
endif()
