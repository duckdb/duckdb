# FIXME: disabled for now while CopyFunction undergoes heavy changes
if (NOT DEFINED VORTEX_ENABLED)
    set(VORTEX_ENABLED ON)
endif()
if (VORTEX_ENABLED AND NOT WIN32 AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(vortex
            GIT_URL https://github.com/vortex-data/duckdb-vortex
            GIT_TAG e4b9a57895ea007cf0d92480e406f7a1908a308b
            SUBMODULES vortex
            APPLY_PATCHES
            LOAD_TESTS
    )
endif()
