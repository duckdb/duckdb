if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 755898cd8469c1d2bcb69989e7c80c024dc031cd
            SUBMODULES extension-ci-tools
  )
endif()
