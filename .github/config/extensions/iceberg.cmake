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
            GIT_TAG 63fe159100b9cd4e79e31ff4314353d29b3e6a82
            APPLY_PATCHES
            )
endif()
