if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 3dd16be1ac5153834d6bc88dbbc05807ce4276d7
	    SUBMODULES "third_party/avro-c"
    )
endif()
