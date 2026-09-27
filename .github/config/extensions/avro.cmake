if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 35fa8441d52ce6c9c5af87fa050616e8564dacf8
	    SUBMODULES "third_party/avro-c"
    )
endif()
