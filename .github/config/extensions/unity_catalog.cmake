if (NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
#    duckdb_extension_load(unity_catalog
#            GIT_URL https://github.com/duckdb/unity_catalog
#            GIT_TAG 8a65e9c446cfaa8e3e9502007ecac10190bbe91d
#            LOAD_TESTS
#            )
endif()
