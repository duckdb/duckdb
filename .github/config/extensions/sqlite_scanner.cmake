# Static linking on windows does not properly work due to symbol collision
if (WIN32)
    set(STATIC_LINK_SQLITE "DONT_LINK")
else ()
    set(STATIC_LINK_SQLITE "")
endif()

duckdb_extension_load(sqlite_scanner
        ${STATIC_LINK_SQLITE} LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG 2fc791837413a57198c47f18c73fdf8997af2b4b
        APPLY_PATCHES
        )
