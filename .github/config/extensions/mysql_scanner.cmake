# FIXME: disabled for now, the patches do not apply to the pinned GIT_TAG
if (NOT DEFINED MYSQL_SCANNER_ENABLED)
    set(MYSQL_SCANNER_ENABLED OFF)
endif()
if (MYSQL_SCANNER_ENABLED AND NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(mysql_scanner
            DONT_LINK
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-mysql
            GIT_TAG 3fffdd5ce86fd45b0599246be1994514da4619a3
            SUBMODULES database-connector
            APPLY_PATCHES
            )
endif()
