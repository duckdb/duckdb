# FIXME: disabled for now while CopyFunction undergoes heavy changes
if (FALSE AND NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG 6ea8bd77fe8e6e814bde11b6981f934fa82ab961
            SUBMODULES vortex
            APPLY_PATCHES
            LOAD_TESTS
            DONT_LINK
    )
endif()
