if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG f9d590297485f0318f480372c70bdd852826e258
    )
endif()
