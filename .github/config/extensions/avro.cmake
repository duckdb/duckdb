if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 4fa0f73f816e9878d5b6a39795e060877766ac1a
    )
endif()
