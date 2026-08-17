if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 10a49159f8e65674474b3bf34b92440f2399850d
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
  )
endif()
