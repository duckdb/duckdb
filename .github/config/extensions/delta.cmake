if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG ba6874e33e0afa8f60db7849b4485b50ac6443e2
            SUBMODULES extension-ci-tools
  )
endif()
