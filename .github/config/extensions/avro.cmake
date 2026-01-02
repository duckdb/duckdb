if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 93da8a19b41eb577add83d0552c6946a16e97c83
            APPLY_PATCHES
    )
endif()
