if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-avro
            GIT_TAG 758fb6063cbb6da49b2e1c16c4044cfcafefc52f
	    SUBMODULES "third_party/avro-c"
            APPLY_PATCHES
    )
endif()
