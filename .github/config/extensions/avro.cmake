if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 859d56d1bcf8e1645a4d6cb905b96ebf327af139
	    SUBMODULES "third_party/avro-c"
    )
endif()
