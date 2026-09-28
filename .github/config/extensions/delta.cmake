if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 6059958c6d47050878349dd7a90d4f71587ea5e0
            SUBMODULES extension-ci-tools
  )
endif()
