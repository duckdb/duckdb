if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG b75cb5cea43d3e2bee6f5d0f377aa99354dd868a
    )
endif()
