if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG d786c8bd414ef417d6d4cb1b4f531dd2e7b8f7c3
	    SUBMODULES "third_party/avro-c"
            APPLY_PATCHES
    )
endif()
