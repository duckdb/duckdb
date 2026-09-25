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
            GIT_TAG a0e903e2578c0707aa0892548ab8ea0cb74f4c0c
            )
endif()
