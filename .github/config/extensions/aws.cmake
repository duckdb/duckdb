
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            APPLY_PATCHES
            ### TODO: re-enable LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 4f318ebd088e464266c511abe2f70bbdeee2fcd8
            )
endif()