if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 376b9aa3b2f4821ce3d5a1c68811773db54fa83f
    )
endif()
