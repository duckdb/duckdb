if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 0eb4902b25404b96c0132794ecc846e650da3170
	    SUBMODULES "third_party/avro-c"
    )
endif()
