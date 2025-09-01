# Windows tests for iceberg currently not working
IF (NOT WIN32)
    set(LOAD_ICEBERG_TESTS "LOAD_TESTS")
else ()
    set(LOAD_ICEBERG_TESTS "")
endif()

if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(iceberg
            ${LOAD_ICEBERG_TESTS} 
            GIT_URL https://github.com/duckdb/duckdb-iceberg
            GIT_TAG 213805555a74528f6117fe563d842649982fa415
            )
endif()
