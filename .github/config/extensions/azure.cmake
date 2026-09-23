if (NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(azure
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-azure
        GIT_TAG 74f47669b7c7cac7bad2c29bcfac8ec8b348110e
        APPLY_PATCHES
  )
endif()
