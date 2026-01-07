# Windows tests for iceberg currently not working
IF (NOT WIN32)
    set(LOAD_ICEBERG_TESTS "LOAD_TESTS")
else ()
    set(LOAD_ICEBERG_TESTS "")
endif()
if (NOT MINGW)
    duckdb_extension_load(iceberg
            ${LOAD_ICEBERG_TESTS}
            APPLY_PATCHES
            GIT_URL https://github.com/duckdb/duckdb-iceberg
            GIT_TAG 90c059e2dd876a483e63a2f1b04ef66fa45a0c85
            )
endif()
