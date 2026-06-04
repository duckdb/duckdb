if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG 3fabe3c8c61fbb17d8360dda8fdf2dbec2d89f8e
            SUBMODULES database-connector
            )
endif()
