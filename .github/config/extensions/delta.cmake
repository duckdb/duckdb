if (NOT MINGW AND NOT ${WASM_ENABLED})
  duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 00210e4e3694fd0c9a0f8f5f8cfa7b4a54046b5c
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
    )
endif()
