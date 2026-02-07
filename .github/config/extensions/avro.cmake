if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 62ed88bc1ea0dd6b09238428d887b4b8689784dd
            APPLY_PATCHES
    )
endif()
