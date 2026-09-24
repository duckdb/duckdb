if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG eb84a0757a96f5c711aa98ece29869bb9433e6c2
            SUBMODULES database-connector
            )
endif()
