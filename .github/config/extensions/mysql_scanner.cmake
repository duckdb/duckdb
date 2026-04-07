if (NOT MINGW AND NOT ${WASM_ENABLED} AND NOT ${MUSL_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG e2760cb11f13a12ce74e8c798c07c48deb97a577
            SUBMODULES database-connector
            )
endif()
