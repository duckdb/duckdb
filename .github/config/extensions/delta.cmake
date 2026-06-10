if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 29d43b99c4918becbd8d1dbaaa13a126bdafed49
            SUBMODULES extension-ci-tools
  )
endif()
