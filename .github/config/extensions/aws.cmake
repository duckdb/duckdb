if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG da0ee1da76a37479901f61d55faa3a705d8d1bd8
            APPLY_PATCHES
            )
endif()
