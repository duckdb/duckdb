IF (NOT WIN32)
    set(LOAD_HTTPFS_TESTS "LOAD_TESTS")
else ()
    set(LOAD_HTTPFS_TESTS "")
endif()
duckdb_extension_load(httpfs
    # TODO: restore once httpfs is fixed
    ${LOAD_HTTPFS_TESTS}
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 041a782b0b33495448a7eaa68973cf8c2174feb6
    INCLUDE_DIR src/include
    APPLY_PATCHES
)
