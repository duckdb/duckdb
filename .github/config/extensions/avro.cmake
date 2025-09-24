if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 0c97a61781f63f8c5444cf3e0c6881ecbaa9fe13
    )
endif()
