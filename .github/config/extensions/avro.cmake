if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG fa61c8e2bfa5b173749bf9db4fc853dea78969b6
    )
endif()
