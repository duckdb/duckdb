# Windows tests for iceberg currently not working
IF (NOT WIN32)
    set(LOAD_ICEBERG_TESTS "LOAD_TESTS")
else ()
    set(LOAD_ICEBERG_TESTS "")
endif()
if (NOT MINGW)
    duckdb_extension_load(iceberg
	    #FIXME: restore autoloading tests ${LOAD_ICEBERG_TESTS}
            GIT_URL https://github.com/duckdb/duckdb-iceberg
            GIT_TAG f25e382e829166487398df1cfbb0a48bdc1873f3
            )
endif()
