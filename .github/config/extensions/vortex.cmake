if (NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG 275ac230e1d9afd08926b6989ec2467f92fae6e3
            SUBMODULES vortex
            APPLY_PATCHES
            LOAD_TESTS
            DONT_LINK
    )
endif()
