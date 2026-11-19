if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 1b96925856ded7c9cbde7403fdb3d467d0cdf779
            )
endif()
