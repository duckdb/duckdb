if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 36a4d8ac56647e0810529a3c725162b0976ee73f
	    SUBMODULES "third_party/avro-c"
    )
endif()
