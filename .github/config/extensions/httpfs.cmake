IF (NOT WIN32)
    set(LOAD_HTTPFS_TESTS "LOAD_TESTS")
else ()
    set(LOAD_HTTPFS_TESTS "")
endif()
duckdb_extension_load(httpfs
    # TODO: restore once httpfs is fixed
    ${LOAD_HTTPFS_TESTS}
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG b80c680f86dff6061614536d908d4b08c85c9ef4
    INCLUDE_DIR src/include
    APPLY_PATCHES
)
