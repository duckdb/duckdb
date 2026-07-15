if (NOT MINGW)
    duckdb_extension_load(avro
            LOAD_TESTS
            GIT_URL https://github.com/Tmonster/duckdb-avro
            GIT_TAG 88240f56cbc2d95c60083170ecac9ba389174356
	    SUBMODULES "third_party/avro-c"
    )
endif()
