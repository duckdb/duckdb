if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 4176191c961fe5c0763976b8b869e30ba0e93054
            SUBMODULES "third_party/avro-c"
            APPLY_PATCHES
    )
endif()
