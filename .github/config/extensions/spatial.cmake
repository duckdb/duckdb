if (NOT MINGW AND ${BUILD_COMPLETE_EXTENSION_SET})
################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-spatial
    GIT_TAG 5bebe0050e2b9eb62fb6155d2eb8b59b4d2eadb0
    INCLUDE_DIR src/spatial
    TEST_DIR test/sql
    )
endif()
