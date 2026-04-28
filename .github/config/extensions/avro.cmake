if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 22611989ede86328e78af2218ee47ffea97a2ae8
    )
endif()
