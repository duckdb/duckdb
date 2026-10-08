if (NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(azure
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-azure
        GIT_TAG 35c55cf13f2fbf79161f507b511b4283382348e4
        APPLY_PATCHES
  )
endif()
