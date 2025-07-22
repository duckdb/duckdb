
if (NOT MINGW)
    duckdb_extension_load(avro
            APPLY_PATCHES
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG ff766174cc6cc9c4ed93fc4b75871bcdffcc6e65
    )
endif()