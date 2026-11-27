if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG ec414ba086b74c293582bd3b3d9d4786cf142a95
            APPLY_PATCHES
            )
endif()
