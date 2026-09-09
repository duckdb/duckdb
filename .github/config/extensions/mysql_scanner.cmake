if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG fe2af21e9434f7aec30bc2a4641337e69d84ab2a
            SUBMODULES database-connector
            APPLY_PATCHES
            )
endif()
