if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG a8ebc6c171b94c80a61f3524ce401b647dc74877
            )
endif()
