if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 3e4ff605a31bc2484bbbc1bfbad82ed366fd74d6
	    SUBMODULES "third_party/avro-c"
    )
endif()
