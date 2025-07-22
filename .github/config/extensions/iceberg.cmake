# Windows tests for iceberg currently not working
IF (NOT WIN32)
    set(LOAD_ICEBERG_TESTS "LOAD_TESTS")
else ()
    set(LOAD_ICEBERG_TESTS "")
endif()

if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(iceberg
            APPLY_PATCHES
#            ${LOAD_ICEBERG_TESTS} TODO: re-enable once autoloading test is fixed
            GIT_URL https://github.com/duckdb/duckdb-iceberg
            GIT_TAG 24dd874bee165661f6c3c79ee2a823f02941ed94
            )
endif()