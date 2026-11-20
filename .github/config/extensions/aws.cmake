if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 9ba41b09a010322008f160af0dfce93941d6f52a
            )
endif()
