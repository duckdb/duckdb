if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG fa09aa71a716703cbd865a4f3cea17224202cde1
	    SUBMODULES "third_party/avro-c"
            APPLY_PATCHES
    )
endif()
