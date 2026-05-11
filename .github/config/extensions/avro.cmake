if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 0535350850786be8a8bc03b9ffc59e29dd32ecf7
    )
endif()
