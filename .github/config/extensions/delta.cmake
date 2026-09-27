if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 1419d1616f7339634a525b009607bdd16d2faf92
            SUBMODULES extension-ci-tools
  )
endif()
