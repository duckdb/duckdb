if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG c462500d74fcb01b43c56e8a0435d516eed49921
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
  )
endif()
