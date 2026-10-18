if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG e9cd304ad55ee08faa3dc546a1f16ac7e7cee3f9
            SUBMODULES extension-ci-tools
  )
endif()
