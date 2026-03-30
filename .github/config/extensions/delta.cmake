if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG df4a08d12f591793c9d2efb3ce6cb91de98978c7
            SUBMODULES extension-ci-tools
  )
endif()
