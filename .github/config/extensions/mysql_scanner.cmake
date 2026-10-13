if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG 66ad12cd0f7e216aa7a1e87f21fb87f6f106d918
            SUBMODULES database-connector
            )
endif()
