if(NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG d1b2ffd7de8acba19fe5151cbfcb5b0d12971aaa
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
  )
endif()
