# Static linking on windows does not properly work due to symbol collision
if (WIN32)
    set(STATIC_LINK_SQLITE "DONT_LINK")
else ()
    set(STATIC_LINK_SQLITE "")
endif()

duckdb_extension_load(sqlite_scanner
        ${STATIC_LINK_SQLITE} LOAD_TESTS APPLY_PATCHES
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG 73254c99b1873a083bbae0640a3d8a7fef431761
        )
