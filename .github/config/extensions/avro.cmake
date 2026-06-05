if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 7f423d69709045e38f8431b3470e0395fce1a595
    )
endif()
