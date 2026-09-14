if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG c40fd3d3e16ad7061f0a32b9affecb6604f71015
            SUBMODULES database-connector
            APPLY_PATCHES
            )
endif()
