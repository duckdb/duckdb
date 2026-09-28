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
            GIT_TAG 890b78a9cfae380396b435b033c27cdbdad04e42
            )
endif()
