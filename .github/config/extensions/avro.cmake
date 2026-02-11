if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG e4bd712795ecb31f97106880c6f2843ffea630f1
    )
endif()
