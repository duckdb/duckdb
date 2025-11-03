# Windows tests for iceberg currently not working
IF (NOT WIN32)
    set(LOAD_ICEBERG_TESTS "LOAD_TESTS")
else ()
    set(LOAD_ICEBERG_TESTS "")
endif()
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(iceberg
#            ${LOAD_ICEBERG_TESTS} TODO: re-enable once autoloading test is fixed
            GIT_URL https://github.com/duckdb/duckdb-iceberg
            GIT_TAG 30a2c66f1040ffd36e117ace6e22acbdb402e79c
            APPLY_PATCHES
            )
endif()
