if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG d9dccdafa15c57071817ea13f1588c7b04f61ba6
    )
endif()
