if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 2d76d40cffa5a8cee04e6fbfb9131c170e471129
            SUBMODULES extension-ci-tools
  )
endif()
