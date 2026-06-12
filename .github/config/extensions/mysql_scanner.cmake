if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG 2a59de314c07bece84ae0be4286c9b8964419b95
            SUBMODULES database-connector
            )
endif()
