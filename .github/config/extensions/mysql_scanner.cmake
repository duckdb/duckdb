if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG 496ac9e3cb61bd8d6d1255f73cf69b958a311525
            SUBMODULES database-connector
            )
endif()
