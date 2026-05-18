if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 23a3beb04df9c9c021c2fde3359acaf52d8d05a3
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
  )
endif()
