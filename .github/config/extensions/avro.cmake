if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 70f1766d8deb91cbf55e452d474755effc14a35b
            APPLY_PATCHES
	    SUBMODULES "third_party/avro-c"
            APPLY_PATCHES
    )
endif()
