if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 776be7ac2c51211e12bcd46c911ab11267cc44f3
            SUBMODULES extension-ci-tools
  )
endif()
