if(NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
  duckdb_extension_load(unity_catalog
            GIT_URL https://github.com/duckdb/unity_catalog
            GIT_TAG 91eff65972b822f418d4c24225bb5b03f1bda2d0
            LOAD_TESTS
            APPLY_PATCHES
  )
endif()
