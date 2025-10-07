if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG 03aaf0f073bc622ade27c158d32473588b32aa8b
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
    )
endif()