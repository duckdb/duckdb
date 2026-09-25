if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG b1618a39cac06c72c8ea366f6b1827d9b8d66903
	    SUBMODULES "third_party/avro-c"
    )
endif()
