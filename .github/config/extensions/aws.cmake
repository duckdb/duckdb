if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 1963bfadf28f3b1c021cbccce1f318af2a338f65
            )
endif()
