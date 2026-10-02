if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG 5c219c915871415785d31cd10762809e7a4f651a
            SUBMODULES database-connector
            )
endif()
