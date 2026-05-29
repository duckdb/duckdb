if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG eea363acf7c4faba84469043cb59895077785cd6
    )
endif()
