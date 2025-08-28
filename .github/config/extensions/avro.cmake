if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 3b9a6dc5858b4c21d20201cc6fa416589ceaf24b
            APPLY_PATCHES
    )
endif()
