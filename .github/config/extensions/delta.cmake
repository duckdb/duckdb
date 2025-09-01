if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(delta
            GIT_URL https://github.com/duckdb/duckdb-delta
            GIT_TAG b0b32dbd30561dfc0db6399f5305fd14c04ec89d # currently latest commit of v1.4-andium branch
            SUBMODULES extension-ci-tools
            APPLY_PATCHES
    )
endif()