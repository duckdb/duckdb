IF (NOT WIN32)
    set(LOAD_HTTPFS_TESTS "LOAD_TESTS")
else ()
    set(LOAD_HTTPFS_TESTS "")
endif()
duckdb_extension_load(httpfs
    # TODO: restore once httpfs is fixed
    ${LOAD_HTTPFS_TESTS}
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 8356a9017444f54018159718c8017ff7db4ea756
    APPLY_PATCHES
    INCLUDE_DIR src/include
)
