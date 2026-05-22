# Windows tests for iceberg currently not working
IF (NOT WIN32)
    set(LOAD_ICEBERG_TESTS "LOAD_TESTS")
else ()
    set(LOAD_ICEBERG_TESTS "")
endif()
if (NOT MINGW)
    duckdb_extension_load(iceberg
	    #FIXME: restore autoloading tests ${LOAD_ICEBERG_TESTS}
            SOURCE_DIR ${CMAKE_SOURCE_DIR}/third_party/duckdb_iceberg
            INCLUDE_DIR ${CMAKE_SOURCE_DIR}/third_party/duckdb_iceberg/src/include
            )
endif()
