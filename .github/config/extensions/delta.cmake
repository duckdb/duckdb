if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG a33adba985f7e418a446a20c26bd6b7f073358a4
            SUBMODULES extension-ci-tools
  )
endif()
