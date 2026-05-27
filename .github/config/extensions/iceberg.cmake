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
            GIT_TAG 3097bccd672deebbb4cef4b08536ba845b3852ac
            )
endif()
